package pro.surpin.data.arrowflight.codec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Measures LZ4_FRAME decompression of 8 MiB batches as a Java Flight client sees it: this codec
 * vs commons-compress (Arrow Java's own), single thread. Prints one line per codec.
 */
@Tag("perf")
class Lz4DecodePerfTest {

    private static final int BYTES = 8 << 20;
    private static final int ROUNDS = 40;

    /**
     * Decodes the same server-written frame with both codecs and prints MB/s.
     */
    @Test
    void decodeThroughput() {
        CompressionCodec ours = new Lz4FrameCodec();
        CompressionCodec commons = CommonsCompressionFactory.INSTANCE.createCodec(
                CompressionUtil.CodecType.LZ4_FRAME);
        try (BufferAllocator allocator = new RootAllocator()) {
            ArrowBuf input = allocator.buffer(BYTES);
            ByteBuffer longs = input.nioBuffer(0, BYTES).order(ByteOrder.LITTLE_ENDIAN);
            Random random = new Random(3);
            for (int i = 0; i < BYTES / Long.BYTES; i++) {
                longs.putLong(i * Long.BYTES, random.nextInt(100_000));
            }
            input.writerIndex(BYTES);
            try (ArrowBuf frame = ours.compress(allocator, input)) {
                for (CompressionCodec codec : new CompressionCodec[]{ours, commons}) {
                    measure(allocator, codec, frame);
                }
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Decodes a frame repeatedly and prints the throughput of the second half of the rounds.
     *
     * @param allocator allocator
     * @param codec codec
     * @param frame compressed buffer, kept alive by an extra reference per round
     */
    private static void measure(BufferAllocator allocator, CompressionCodec codec, ArrowBuf frame) {
        long start = 0;
        for (int round = 0; round < ROUNDS; round++) {
            if (round == ROUNDS / 2) {
                start = System.nanoTime();
            }
            frame.getReferenceManager().retain();
            try (ArrowBuf output = codec.decompress(allocator, frame)) {
                assertEquals(BYTES, output.writerIndex());
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        System.out.printf("lz4Decode codec=%s ratio=%.2f MBps=%.0f%n", codec.getClass().getSimpleName(),
                (double) BYTES / frame.writerIndex(), (ROUNDS / 2) * (BYTES / 1e6) / seconds);
    }
}
