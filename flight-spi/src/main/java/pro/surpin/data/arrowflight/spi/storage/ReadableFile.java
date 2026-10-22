package pro.surpin.data.arrowflight.spi.storage;

import java.io.IOException;

/**
 * A file opened for positional reads, such as Parquet footer reads. The server wraps it as a
 * Parquet input file; it may be read from several threads at once.
 */
public interface ReadableFile {

    /**
     * Returns the file length.
     *
     * @return length in bytes
     */
    long length();

    /**
     * Reads exactly {@code length} bytes starting at a position.
     *
     * @param position first byte to read
     * @param buffer target array
     * @param offset first index of the array to write
     * @param length number of bytes to read
     * @throws java.io.EOFException when the range ends after the end of the file
     * @throws IOException on storage access failure
     */
    void readFully(long position, byte[] buffer, int offset, int length) throws IOException;
}
