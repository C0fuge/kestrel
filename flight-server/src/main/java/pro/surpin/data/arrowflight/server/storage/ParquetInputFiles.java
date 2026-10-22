package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * Turns a store's {@link ReadableFile} into a Parquet {@link InputFile}: every read of the
 * stream is one positional read of the file, so the requests a store sends are the ones its
 * readable file decides on (an S3 file fetches its 64 KB tail once).
 */
public final class ParquetInputFiles {

    /** Not instantiable. */
    private ParquetInputFiles() {
    }

    /**
     * Wraps a readable file.
     *
     * @param file readable file
     * @param name file name for logs and errors
     * @return Parquet input file
     */
    public static InputFile of(ReadableFile file, String name) {
        Objects.requireNonNull(file, "file");
        Objects.requireNonNull(name, "name");
        return new InputFile() {
            @Override
            public long getLength() {
                return file.length();
            }

            @Override
            public SeekableInputStream newStream() {
                return new Stream(file, name);
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    /**
     * A position in a readable file.
     */
    private static final class Stream extends SeekableInputStream {
        private final ReadableFile file;
        private final String name;
        private final long length;
        private long position;
        private boolean closed;

        /**
         * Creates a stream at the start of a file.
         *
         * @param file readable file
         * @param name file name for errors
         */
        Stream(ReadableFile file, String name) {
            this.file = file;
            this.name = name;
            this.length = file.length();
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPosition) throws IOException {
            if (newPosition < 0 || newPosition > length) {
                throw new EOFException("Seek to " + newPosition + " outside " + name
                        + " of length " + length);
            }
            position = newPosition;
        }

        @Override
        public int read() throws IOException {
            if (position >= length) {
                return -1;
            }
            byte[] one = new byte[1];
            fill(one, 0, 1);
            return one[0] & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, bytes.length);
            if (count == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int n = (int) Math.min(count, length - position);
            fill(bytes, offset, n);
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int offset, int count) throws IOException {
            Objects.checkFromIndexSize(offset, count, bytes.length);
            if (count > length - position) {
                throw new EOFException("Read of " + count + " bytes at " + position
                        + " beyond the end of " + name);
            }
            fill(bytes, offset, count);
        }

        @Override
        public int read(ByteBuffer target) throws IOException {
            if (!target.hasRemaining()) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int n = (int) Math.min(target.remaining(), length - position);
            byte[] bytes = new byte[n];
            fill(bytes, 0, n);
            target.put(bytes);
            return n;
        }

        @Override
        public void readFully(ByteBuffer target) throws IOException {
            int n = target.remaining();
            if (n > length - position) {
                throw new EOFException("Read of " + n + " bytes at " + position
                        + " beyond the end of " + name);
            }
            byte[] bytes = new byte[n];
            fill(bytes, 0, n);
            target.put(bytes);
        }

        @Override
        public long skip(long n) {
            long skipped = Math.max(0, Math.min(n, length - position));
            position += skipped;
            return skipped;
        }

        @Override
        public void close() {
            closed = true;
        }

        /**
         * Reads bytes at the current position and moves past them.
         *
         * @param bytes target array
         * @param offset first index to write
         * @param count number of bytes, all within the file
         * @throws IOException on storage access failure, or when closed
         */
        private void fill(byte[] bytes, int offset, int count) throws IOException {
            if (closed) {
                throw new IOException("Stream of " + name + " is closed");
            }
            file.readFully(position, bytes, offset, count);
            position += count;
        }
    }
}
