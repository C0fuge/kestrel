package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests ranged reads of an S3 object. */
@Tag("unit")
class S3InputFileTest {

    private static final int LENGTH = 300_000;

    private final byte[] data = new byte[LENGTH];
    private final List<long[]> requests = new ArrayList<>();

    S3InputFileTest() {
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31);
        }
    }

    /** Verifies the last 8 bytes and a footer within the last 64 KB cost one request. */
    @Test
    void footerReadFetchesTheTailOnce() throws Exception {
        try (SeekableInputStream in = file().newStream()) {
            byte[] tail = new byte[8];
            in.seek(LENGTH - 8);
            in.readFully(tail);
            byte[] footer = new byte[40_000];
            in.seek(LENGTH - 8 - footer.length);
            in.readFully(footer);

            assertArrayEquals(Arrays.copyOfRange(data, LENGTH - 8, LENGTH), tail);
            assertArrayEquals(Arrays.copyOfRange(data, LENGTH - 8 - footer.length, LENGTH - 8), footer);
            assertEquals(1, requests.size());
            assertEquals(LENGTH - S3InputFile.BLOCK_SIZE, requests.get(0)[0]);
            assertEquals(S3InputFile.BLOCK_SIZE, requests.get(0)[1]);
        }
    }

    /** Verifies a read before the tail fetches at least one block from its position. */
    @Test
    void readsBeforeTheTail() throws Exception {
        try (SeekableInputStream in = file().newStream()) {
            in.seek(1000);
            assertEquals(data[1000] & 0xff, in.read());
            ByteBuffer buffer = ByteBuffer.allocate(100_000);
            in.seek(5000);
            in.readFully(buffer);

            assertArrayEquals(Arrays.copyOfRange(data, 5000, 105_000), buffer.array());
            assertEquals(105_000, in.getPos());
            assertEquals(1000, requests.get(0)[0]);
            assertEquals(S3InputFile.BLOCK_SIZE, requests.get(0)[1]);
        }
    }

    /** Verifies reads past the end are refused or return -1. */
    @Test
    void endOfFile() throws Exception {
        try (SeekableInputStream in = file().newStream()) {
            in.seek(LENGTH);
            assertEquals(-1, in.read());
            assertEquals(-1, in.read(new byte[4], 0, 4));
            in.seek(LENGTH - 2);
            assertThrows(EOFException.class, () -> in.readFully(new byte[4]));
            assertThrows(EOFException.class, () -> in.seek(LENGTH + 1));
            byte[] two = new byte[4];
            assertEquals(2, in.read(two, 0, 4));
        }
    }

    private S3InputFile file() {
        return new S3InputFile((start, count) -> {
            requests.add(new long[] {start, count});
            return Arrays.copyOfRange(data, (int) start, (int) start + count);
        }, "s3://lake/x.parquet", LENGTH);
    }
}
