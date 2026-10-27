package pro.surpin.data.arrowflight.server.adapters;

import java.util.SplittableRandom;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Measures single-thread compress and decompress speed and ratio of the stream codecs on 8 MiB
 * of small longs (opt-in: perf tag).
 */
@Tag("perf")
class StreamCompressionPerfTest {

    private static final int BYTES = 8 << 20;
    private static final int ROUNDS = 10;

    /**
     * Prints MB/s and ratio per codec.
     */
    @Test
    void codecSpeed() {
        try (BufferAllocator allocator = new RootAllocator()) {
            for (StreamCompression.Codec codec : new StreamCompression.Codec[]{
                    StreamCompression.Codec.ZSTD, StreamCompression.Codec.LZ4}) {
                CompressionCodec arrow = CompressingStreamListener.arrowCodec(codec, 1);
                long compressNanos = 0;
                long decompressNanos = 0;
                long compressedBytes = 0;
                for (int round = 0; round <= ROUNDS; round++) {
                    ArrowBuf input = longs(allocator);
                    long start = System.nanoTime();
                    ArrowBuf compressed = arrow.compress(allocator, input);
                    long middle = System.nanoTime();
                    long size = compressed.writerIndex();
                    ArrowBuf output = arrow.decompress(allocator, compressed);
                    long end = System.nanoTime();
                    output.close();
                    if (round > 0) {
                        compressNanos += middle - start;
                        decompressNanos += end - middle;
                        compressedBytes += size;
                    }
                }
                double mb = (double) BYTES * ROUNDS / 1e6;
                System.out.printf("%s: compress %.0f MB/s, decompress %.0f MB/s, ratio %.2f%n", codec,
                        mb / (compressNanos / 1e9), mb / (decompressNanos / 1e9),
                        (double) BYTES * ROUNDS / compressedBytes);
                assertTrue(compressedBytes < (long) BYTES * ROUNDS, codec.name());
            }
        }
    }

    /**
     * Fills a buffer with pseudo-random longs 0..999, as a BIGINT column of small values.
     *
     * @param allocator allocator
     * @return buffer of 8 MiB
     */
    private static ArrowBuf longs(BufferAllocator allocator) {
        ArrowBuf buf = allocator.buffer(BYTES);
        SplittableRandom random = new SplittableRandom(42);
        for (int i = 0; i < BYTES / Long.BYTES; i++) {
            buf.setLong((long) i * Long.BYTES, random.nextInt(1000));
        }
        buf.writerIndex(BYTES);
        return buf;
    }
}
