package pro.surpin.data.arrowflight.server.storage;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * One S3 object read for its Parquet footer. Every read outside the bytes fetched so far is one
 * ranged GET of at least {@link #BLOCK_SIZE} bytes; a read near the end fetches the whole last
 * block, so a footer of up to 64 KB costs one request (Parquet reads the last 8 bytes for the
 * footer length, then the footer).
 */
final class S3ReadableFile implements ReadableFile {

    /** Bytes fetched per request at least, and the tail fetched for a footer. */
    static final int BLOCK_SIZE = 64 * 1024;

    private final RangeReader reader;
    private final String name;
    private final long length;
    /** First byte of the block fetched last. */
    private long bufferStart;
    /** The block fetched last. */
    private byte[] buffer = new byte[0];

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
     * Creates a readable file of known length.
     *
     * @param reader reads byte ranges of the object
     * @param name object URI, for logs
     * @param length object length in bytes
     */
    S3ReadableFile(RangeReader reader, String name, long length) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.name = Objects.requireNonNull(name, "name");
        this.length = length;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public synchronized void readFully(long position, byte[] bytes, int offset, int count)
            throws IOException {
        Objects.checkFromIndexSize(offset, count, bytes.length);
        if (position < 0 || count > length - position) {
            throw new EOFException("Read of " + count + " bytes at " + position
                    + " beyond the end of " + name);
        }
        long at = position;
        int done = 0;
        while (done < count) {
            ensureBuffered(at, count - done);
            int from = (int) (at - bufferStart);
            int n = Math.min(count - done, buffer.length - from);
            System.arraycopy(buffer, from, bytes, offset + done, n);
            done += n;
            at += n;
        }
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * Fetches the block that holds a position unless it is buffered already.
     *
     * @param position byte the caller reads next
     * @param wanted bytes the caller wants from that position
     * @throws IOException on storage access failure
     */
    private void ensureBuffered(long position, int wanted) throws IOException {
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
