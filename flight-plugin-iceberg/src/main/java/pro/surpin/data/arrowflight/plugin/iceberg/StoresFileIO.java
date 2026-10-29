package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * Read-only Iceberg FileIO over the server's stores: table metadata, manifest lists and
 * manifests are read through the store whose root holds them, never through a file system of
 * this plugin's own. A table whose catalog vended S3 credentials reads the files below a
 * credential's prefix (and below one of the catalog's roots) with those credentials instead
 * ({@link #withCredentials}), so the server's own keys need not reach the warehouse. Writing and
 * deleting are refused.
 */
final class StoresFileIO implements FileIO {

    private static final long serialVersionUID = 1L;
    /** Bytes read from the store at a time. */
    private static final int BLOCK = 256 * 1024;

    /** Connect timeout of reads with vended credentials. */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private final transient Stores stores;
    /** Readers of the vended credentials, longest prefix first; empty for the server's stores only. */
    private final transient List<VendedS3> vended;
    /** Normalised roots a vended read must lie below. */
    private final List<String> roots;

    /**
     * Creates the FileIO.
     *
     * @param stores the server's stores
     */
    StoresFileIO(Stores stores) {
        this(stores, List.of(), List.of());
    }

    /**
     * Creates the FileIO.
     *
     * @param stores the server's stores
     * @param vended readers of vended credentials
     * @param roots roots a vended read must lie below
     */
    private StoresFileIO(Stores stores, List<VendedS3> vended, List<String> roots) {
        this.stores = stores;
        this.vended = vended;
        this.roots = List.copyOf(roots);
    }

    /**
     * Returns a FileIO that reads the files below the prefixes of some vended S3 credentials
     * with those credentials (the longest matching prefix wins), and every other file through
     * the stores. A file below a prefix but outside every root is read through the stores, as
     * without credentials (and so fails outside the stores' roots).
     *
     * @param credentials the table's vended credentials
     * @param catalogRoots the catalog's readable roots, normalised
     * @param timeout request timeout
     * @return this FileIO without usable S3 credentials, else a new one
     */
    StoresFileIO withCredentials(List<StorageCredential> credentials, List<String> catalogRoots,
            Duration timeout) {
        return withCredentials(credentials, catalogRoots, Http.CLIENT, timeout, Clock.systemUTC());
    }

    /**
     * Returns a FileIO with vended credentials, see {@link #withCredentials(List, List, Duration)}.
     *
     * @param credentials the table's vended credentials
     * @param catalogRoots the catalog's readable roots, normalised
     * @param http HTTP client
     * @param timeout request timeout
     * @param clock clock for request times
     * @return this FileIO without usable S3 credentials, else a new one
     */
    StoresFileIO withCredentials(List<StorageCredential> credentials, List<String> catalogRoots,
            HttpClient http, Duration timeout, Clock clock) {
        List<VendedS3> readers = new ArrayList<>();
        for (StorageCredential credential : credentials) {
            VendedS3.of(credential, http, timeout, clock).ifPresent(readers::add);
        }
        if (readers.isEmpty()) {
            return this;
        }
        readers.sort((a, b) -> Integer.compare(b.prefix().length(), a.prefix().length()));
        return new StoresFileIO(stores, List.copyOf(readers), catalogRoots);
    }

    /**
     * Refuses deserialisation: this FileIO reads through the server's stores, which do not
     * travel; Iceberg serialises a FileIO only for engines that ship tasks.
     *
     * @param in stream
     * @throws java.io.NotSerializableException always
     */
    private void readObject(java.io.ObjectInputStream in) throws java.io.NotSerializableException {
        throw new java.io.NotSerializableException(StoresFileIO.class.getName());
    }

    /**
     * Finds the vended credential a location is read with.
     *
     * @param location normalised location
     * @return reader, null when the stores read it
     */
    VendedS3 vendedFor(String location) {
        if (!Locations.underAny(location, roots)) {
            return null;
        }
        for (VendedS3 reader : vended) {
            if (reader.covers(location)) {
                return reader;
            }
        }
        return null;
    }

    @Override
    public InputFile newInputFile(String path) {
        return new StoreInputFile(path, -1);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new StoreInputFile(path, length);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException("The Flight server reads Iceberg tables only");
    }

    @Override
    public void deleteFile(String path) {
        throw new UnsupportedOperationException("The Flight server reads Iceberg tables only");
    }

    /**
     * A file found through the stores; opened on first use.
     */
    private final class StoreInputFile implements InputFile {
        private final String location;
        private long length;
        private ReadableFile file;

        /**
         * Creates the input file.
         *
         * @param location location as Iceberg names it
         * @param length known length, or -1
         */
        StoreInputFile(String location, long length) {
            this.location = location;
            this.length = length;
        }

        /**
         * Opens the file through its store.
         *
         * @return readable file
         */
        private synchronized ReadableFile open() {
            if (file == null) {
                String normalized = Locations.normalize(location);
                VendedS3 reader = vendedFor(normalized);
                if (reader != null) {
                    file = vendedFile(reader, normalized);
                    length = file.length();
                    return file;
                }
                try {
                    Located located = stores.locate(normalized);
                    file = located.store().open(located.relativePath());
                    length = file.length();
                } catch (java.io.FileNotFoundException e) {
                    throw new NotFoundException(e, "File does not exist: %s", location);
                } catch (IOException e) {
                    throw new UncheckedIOException("Cannot open " + location, e);
                }
            }
            return file;
        }

        @Override
        public long getLength() {
            synchronized (this) {
                if (length >= 0) {
                    return length;
                }
            }
            return open().length();
        }

        @Override
        public SeekableInputStream newStream() {
            return new Stream(open());
        }

        @Override
        public String location() {
            return location;
        }

        /**
         * Opens a file with vended credentials.
         *
         * @param reader reader of the credential
         * @param normalized normalised location
         * @return readable file
         */
        private ReadableFile vendedFile(VendedS3 reader, String normalized) {
            long size;
            try {
                size = length >= 0 ? length : reader.length(normalized);
            } catch (java.io.FileNotFoundException e) {
                throw new NotFoundException(e, "File does not exist: %s", location);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot open " + location, e);
            }
            return new ReadableFile() {
                @Override
                public long length() {
                    return size;
                }

                @Override
                public void readFully(long position, byte[] buffer, int offset, int count)
                        throws IOException {
                    reader.readFully(normalized, position, buffer, offset, count);
                }
            };
        }

        @Override
        public boolean exists() {
            String normalized = Locations.normalize(location);
            VendedS3 reader = vendedFor(normalized);
            try {
                if (reader != null) {
                    return reader.exists(normalized);
                }
                Located located = stores.locate(normalized);
                return located.store().exists(located.relativePath());
            } catch (IOException e) {
                return false;
            }
        }
    }

    /**
     * The HTTP client of vended reads, created on first use.
     */
    private static final class Http {
        static final HttpClient CLIENT = HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NEVER).build();

        /** Not instantiable. */
        private Http() {
        }
    }

    /**
     * Sequential and positioned reads of a store file, a block at a time.
     */
    private static final class Stream extends SeekableInputStream {
        private final ReadableFile file;
        private final byte[] buffer = new byte[BLOCK];
        /** File position of buffer[0]. */
        private long bufferStart;
        private int bufferLength;
        private long position;
        private boolean closed;

        /**
         * Creates the stream.
         *
         * @param file open file
         */
        Stream(ReadableFile file) {
            this.file = file;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException {
            if (newPosition < 0) {
                throw new EOFException("Negative position " + newPosition);
            }
            position = newPosition;
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (length == 0) {
                return 0;
            }
            long size = file.length();
            if (position >= size) {
                return -1;
            }
            if (position < bufferStart || position >= bufferStart + bufferLength) {
                bufferStart = position;
                bufferLength = (int) Math.min(BLOCK, size - position);
                file.readFully(bufferStart, buffer, 0, bufferLength);
            }
            int start = (int) (position - bufferStart);
            int n = Math.min(length, bufferLength - start);
            System.arraycopy(buffer, start, target, offset, n);
            position += n;
            return n;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
