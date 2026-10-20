package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the Parquet stream over a store's positional reads. */
@Tag("unit")
class ParquetInputFilesTest {

    private static final int LENGTH = 1000;
    private final byte[] data = new byte[LENGTH];
    private final List<long[]> reads = new ArrayList<>();

    ParquetInputFilesTest() {
        for (int i = 0; i < LENGTH; i++) {
            data[i] = (byte) (i * 7);
        }
    }

    @Test
    void streamReadsAreOnePositionalReadEach() throws Exception {
        try (SeekableInputStream in = ParquetInputFiles.of(file(), "f").newStream()) {
            assertEquals(LENGTH, ParquetInputFiles.of(file(), "f").getLength());
            in.seek(LENGTH - 8);
            byte[] tail = new byte[8];
            in.readFully(tail);
            assertArrayEquals(Arrays.copyOfRange(data, LENGTH - 8, LENGTH), tail);
            assertEquals(LENGTH, in.getPos());
            in.seek(10);
            assertEquals(data[10] & 0xff, in.read());
            ByteBuffer buffer = ByteBuffer.allocate(100);
            in.readFully(buffer);
            assertArrayEquals(Arrays.copyOfRange(data, 11, 111), buffer.array());
            assertEquals(3, reads.size());
            assertEquals(5, in.skip(5));
            assertEquals(116, in.getPos());
        }
    }

    @Test
    void endOfFile() throws Exception {
        try (SeekableInputStream in = ParquetInputFiles.of(file(), "f").newStream()) {
            in.seek(LENGTH);
            assertEquals(-1, in.read());
            assertEquals(-1, in.read(new byte[4], 0, 4));
            assertEquals(-1, in.read(ByteBuffer.allocate(4)));
            in.seek(LENGTH - 2);
            assertThrows(EOFException.class, () -> in.readFully(new byte[4]));
            assertThrows(EOFException.class, () -> in.seek(LENGTH + 1));
            byte[] two = new byte[4];
            assertEquals(2, in.read(two, 0, 4));
        }
    }

    private ReadableFile file() {
        return new ReadableFile() {
            @Override
            public long length() {
                return LENGTH;
            }

            @Override
            public void readFully(long position, byte[] buffer, int offset, int length)
                    throws EOFException {
                if (position + length > LENGTH) {
                    throw new EOFException();
                }
                reads.add(new long[] {position, length});
                System.arraycopy(data, (int) position, buffer, offset, length);
            }
        };
    }
}
