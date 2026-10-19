package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * One S3 object read for its Parquet footer. Every read outside the bytes fetched so far is one
 * ranged GET of at least {@link #BLOCK_SIZE} bytes; a read near the end fetches the whole last
 * block, so a footer of up to 64 KB costs one request (Parquet reads the last 8 bytes for the
 * footer length, then the footer).
 */
final class S3InputFile implements InputFile {

    /** Bytes fetched per request at least, and the tail fetched for a footer. */
    static final int BLOCK_SIZE = 64 * 1024;

    private final RangeReader reader;
    private final String name;
    private final long length;

    /**
     * Reads byte ranges of one object.
     */
    @FunctionalInterface
    interface RangeReader {
        /**
         * Reads a byte range.
         *
         * @param start first byte
         * @param count number of bytes, at least one
         * @return exactly the bytes asked for
         * @throws IOException on storage access failure
         */
        byte[] read(long start, int count) throws IOException;
    }

    /**
     * Creates an input file of known length.
     *
     * @param reader reads byte ranges of the object
     * @param name object URI, for logs
     * @param length object length in bytes
     */
    S3InputFile(RangeReader reader, String name, long length) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.name = Objects.requireNonNull(name, "name");
        this.length = length;
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new Stream();
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * A position in the object and the block fetched last.
     */
    private final class Stream extends SeekableInputStream {
        private long position;
        private long bufferStart;
        private byte[] buffer = new byte[0];
        private boolean closed;

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
            ensureBuffered(1);
            int value = buffer[(int) (position - bufferStart)] & 0xff;
            position++;
            return value;
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
            copy(bytes, offset, n);
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
            copy(bytes, offset, count);
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
            copy(bytes, 0, n);
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
            copy(bytes, 0, n);
            target.put(bytes);
        }

        @Override
        public long skip(long n) {
            long skipped = Math.max(0, Math.min(n, length - position));
            position += skipped;
            return skipped;
        }

        @Override
        public int available() {
            long buffered = bufferStart + buffer.length - position;
            return position >= bufferStart && buffered > 0 ? (int) buffered : 0;
        }

        @Override
        public void close() {
            closed = true;
            buffer = new byte[0];
        }

        /**
         * Copies bytes from the current position, which must lie within the object.
         *
         * @param bytes target array
         * @param offset first index to write
         * @param count number of bytes
         * @throws IOException on storage access failure
         */
        private void copy(byte[] bytes, int offset, int count) throws IOException {
            int done = 0;
            while (done < count) {
                ensureBuffered(count - done);
                int from = (int) (position - bufferStart);
                int n = Math.min(count - done, buffer.length - from);
                System.arraycopy(buffer, from, bytes, offset + done, n);
                done += n;
                position += n;
            }
        }

        /**
         * Fetches the block that holds the current position unless it is buffered already.
         *
         * @param wanted bytes the caller wants from the current position
         * @throws IOException on storage access failure, or when closed
         */
        private void ensureBuffered(int wanted) throws IOException {
            if (closed) {
                throw new IOException("Stream of " + name + " is closed");
            }
            if (position >= bufferStart && position < bufferStart + buffer.length) {
                return;
            }
            long start;
            long end;
            if (length - position <= BLOCK_SIZE) {
                // The footer: fetch the whole tail, the footer itself comes from it next.
                start = Math.max(0, length - BLOCK_SIZE);
                end = length;
            } else {
                start = position;
                end = Math.min(length, position + Math.max(wanted, BLOCK_SIZE));
            }
            int count = (int) Math.min(Integer.MAX_VALUE - 16, end - start);
            byte[] fetched = reader.read(start, count);
            if (fetched.length != count) {
                throw new IOException("Expected " + count + " bytes at " + start + " of " + name
                        + ", got " + fetched.length);
            }
            buffer = fetched;
            bufferStart = start;
        }
    }
}
