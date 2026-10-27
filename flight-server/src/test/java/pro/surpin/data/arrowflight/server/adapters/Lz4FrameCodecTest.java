package pro.surpin.data.arrowflight.server.adapters;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the lz4-java LZ4_FRAME codec: round trips, frames another LZ4 implementation
 * (commons-compress, which Java Flight clients decode with) reads and writes, empty and
 * incompressible buffers, and corrupt input.
 */
@Tag("unit")
class Lz4FrameCodecTest {

    /**
     * Verifies lz4-java is on the test class path, as it is in the runtime libs set.
     */
    @Test
    void isAvailable() {
        assertTrue(Lz4FrameCodec.available());
        assertEquals(CompressionUtil.CodecType.LZ4_FRAME, new Lz4FrameCodec().getCodecType());
    }

    /**
     * Verifies compressible data shrinks and decodes to the same bytes with this codec and with
     * commons-compress, also across several 4 MiB blocks.
     */
    @Test
    void framesDecodeWithBothImplementations() {
        CompressionCodec ours = new Lz4FrameCodec();
        CompressionCodec commons = CommonsCompressionFactory.INSTANCE.createCodec(
                CompressionUtil.CodecType.LZ4_FRAME);
        try (BufferAllocator allocator = new RootAllocator()) {
            for (int size : new int[]{1, 1000, 1 << 20, 9 * (1 << 20) + 17}) {
                byte[] data = compressible(size);
                byte[] viaOurs = roundTrip(allocator, ours, ours, data, size > 1000);
                assertArrayEquals(data, viaOurs, "ours -> ours " + size);
                assertArrayEquals(data, roundTrip(allocator, ours, commons, data, size > 1000),
                        "ours -> commons-compress " + size);
                if (size <= 1 << 20) {
                    // commons-compress compresses far too slowly for the 9 MiB case.
                    assertArrayEquals(data, roundTrip(allocator, commons, ours, data, false),
                            "commons-compress -> ours " + size);
                }
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies an empty buffer and an incompressible one (sent raw, as Arrow's codecs do)
     * round-trip.
     */
    @Test
    void emptyAndIncompressibleBuffersRoundTrip() {
        CompressionCodec codec = new Lz4FrameCodec();
        byte[] random = new byte[256 * 1024];
        new Random(7).nextBytes(random);
        try (BufferAllocator allocator = new RootAllocator()) {
            assertArrayEquals(new byte[0], roundTrip(allocator, codec, codec, new byte[0], false));
            assertArrayEquals(random, roundTrip(allocator, codec, codec, random, false));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies a corrupt frame fails instead of decoding to wrong bytes.
     */
    @Test
    void corruptFrameFails() {
        CompressionCodec codec = new Lz4FrameCodec();
        byte[] data = compressible(64 * 1024);
        try (BufferAllocator allocator = new RootAllocator()) {
            ArrowBuf input = buffer(allocator, data);
            ArrowBuf compressed = codec.compress(allocator, input);
            // Shorten the frame: the announced length can no longer be filled.
            compressed.writerIndex(compressed.writerIndex() - 6);
            assertThrows(RuntimeException.class, () -> codec.decompress(allocator, compressed));
            int refs = compressed.getReferenceManager().getRefCount();
            if (refs > 0) {
                compressed.getReferenceManager().release(refs);
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Compresses with one codec and decompresses with another.
     *
     * @param allocator allocator
     * @param compressor codec that compresses
     * @param decompressor codec that decompresses
     * @param data input bytes
     * @param mustShrink whether the compressed buffer must be smaller than the input
     * @return decompressed bytes
     */
    private static byte[] roundTrip(BufferAllocator allocator, CompressionCodec compressor,
            CompressionCodec decompressor, byte[] data, boolean mustShrink) {
        ArrowBuf input = buffer(allocator, data);
        ArrowBuf compressed = compressor.compress(allocator, input);
        if (mustShrink) {
            assertTrue(compressed.writerIndex() < data.length / 2,
                    compressed.writerIndex() + " of " + data.length);
        }
        try (ArrowBuf output = decompressor.decompress(allocator, compressed)) {
            byte[] bytes = new byte[(int) output.writerIndex()];
            output.getBytes(0, bytes);
            return bytes;
        }
    }

    /**
     * Copies bytes into a new Arrow buffer.
     *
     * @param allocator allocator
     * @param data bytes
     * @return buffer with its writer index at the end
     */
    private static ArrowBuf buffer(BufferAllocator allocator, byte[] data) {
        ArrowBuf buf = allocator.buffer(Math.max(1, data.length));
        buf.setBytes(0, data);
        buf.writerIndex(data.length);
        return buf;
    }

    /**
     * Builds repetitive text.
     *
     * @param size byte count
     * @return bytes
     */
    private static byte[] compressible(int size) {
        StringBuilder text = new StringBuilder(size + 32);
        for (int i = 0; text.length() < size; i++) {
            text.append("row-").append(i % 1000).append(';');
        }
        return text.substring(0, size).getBytes(StandardCharsets.US_ASCII);
    }
}
