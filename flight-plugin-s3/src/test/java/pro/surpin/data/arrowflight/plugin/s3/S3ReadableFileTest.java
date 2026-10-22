package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests ranged reads of an S3 object. */
@Tag("unit")
class S3ReadableFileTest {

    private static final int LENGTH = 300_000;

    private final byte[] data = new byte[LENGTH];
    private final List<long[]> requests = new ArrayList<>();

    S3ReadableFileTest() {
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31);
        }
    }

    /** Verifies the last 8 bytes and a footer within the last 64 KB cost one request. */
    @Test
    void footerReadFetchesTheTailOnce() throws Exception {
        S3ReadableFile file = readable();
        byte[] tail = new byte[8];
        file.readFully(LENGTH - 8, tail, 0, 8);
        byte[] footer = new byte[40_000];
        file.readFully(LENGTH - 8 - footer.length, footer, 0, footer.length);

        assertArrayEquals(Arrays.copyOfRange(data, LENGTH - 8, LENGTH), tail);
        assertArrayEquals(Arrays.copyOfRange(data, LENGTH - 8 - footer.length, LENGTH - 8), footer);
        assertEquals(1, requests.size());
        assertEquals(LENGTH - S3ReadableFile.BLOCK_SIZE, requests.get(0)[0]);
        assertEquals(S3ReadableFile.BLOCK_SIZE, requests.get(0)[1]);
    }

    /** Verifies a read before the tail fetches at least one block from its position. */
    @Test
    void readsBeforeTheTail() throws Exception {
        S3ReadableFile file = readable();
        byte[] one = new byte[1];
        file.readFully(1000, one, 0, 1);
        assertEquals(data[1000], one[0]);
        byte[] buffer = new byte[100_000];
        file.readFully(5000, buffer, 0, buffer.length);

        assertArrayEquals(Arrays.copyOfRange(data, 5000, 105_000), buffer);
        assertEquals(1000, requests.get(0)[0]);
        assertEquals(S3ReadableFile.BLOCK_SIZE, requests.get(0)[1]);
    }

    /** Verifies reads past the end are refused. */
    @Test
    void endOfFile() throws Exception {
        S3ReadableFile file = readable();
        assertThrows(EOFException.class, () -> file.readFully(LENGTH - 2, new byte[4], 0, 4));
        assertThrows(EOFException.class, () -> file.readFully(LENGTH, new byte[1], 0, 1));
        file.readFully(LENGTH, new byte[0], 0, 0);
        assertEquals(0, requests.size());
    }

    /** Verifies positional reads straight on the readable file, across blocks. */
    @Test
    void positionalReadsAcrossBlocks() throws Exception {
        S3ReadableFile file = readable();
        byte[] bytes = new byte[150_000];
        file.readFully(10, bytes, 0, bytes.length);

        assertArrayEquals(Arrays.copyOfRange(data, 10, 150_010), bytes);
        assertEquals(1, requests.size());
        assertEquals(150_000, requests.get(0)[1]);
        assertThrows(EOFException.class, () -> file.readFully(LENGTH - 1, new byte[2], 0, 2));
        assertThrows(EOFException.class, () -> file.readFully(-1, new byte[2], 0, 2));
    }

    private S3ReadableFile readable() {
        return new S3ReadableFile((start, count) -> {
            requests.add(new long[] {start, count});
            return Arrays.copyOfRange(data, (int) start, (int) start + count);
        }, "s3://lake/x.parquet", LENGTH);
    }
}
