package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * Read-only Iceberg FileIO over the server's stores: table metadata, manifest lists and
 * manifests are read through the store whose root holds them, never through a file system of
 * this plugin's own. Writing and deleting are refused.
 */
final class StoresFileIO implements FileIO {

    private static final long serialVersionUID = 1L;
    /** Bytes read from the store at a time. */
    private static final int BLOCK = 256 * 1024;

    private final transient Stores stores;

    /**
     * Creates the FileIO.
     *
     * @param stores the server's stores
     */
    StoresFileIO(Stores stores) {
        this.stores = stores;
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
                try {
                    Located located = stores.locate(Locations.normalize(location));
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

        @Override
        public boolean exists() {
            try {
                Located located = stores.locate(Locations.normalize(location));
                return located.store().exists(located.relativePath());
            } catch (IOException e) {
                return false;
            }
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
