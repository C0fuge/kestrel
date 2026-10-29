package pro.surpin.data.arrowflight.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import net.jpountz.lz4.LZ4FrameOutputStream;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the lz4-java LZ4_FRAME codec: round trips, frames another LZ4 implementation
 * (commons-compress, which Java Flight clients decode with) reads and writes, empty and
 * incompressible buffers, frames of other writers (block by block or through the frame stream),
 * the service-provider factory, and corrupt input.
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
     * Verifies its own and commons-compress's frames (content checksum) are decoded block by block,
     * not through the frame stream.
     */
    @Test
    void ownAndCommonsFramesDecodeBlockByBlock() {
        CompressionCodec ours = new Lz4FrameCodec();
        CompressionCodec commons = CommonsCompressionFactory.INSTANCE.createCodec(
                CompressionUtil.CodecType.LZ4_FRAME);
        byte[] data = compressible(9 * (1 << 20) + 17);
        byte[] small = compressible(200 * 1024);
        try (BufferAllocator allocator = new RootAllocator()) {
            assertTrue(decodesBlockByBlock(allocator, ours, data));
            assertTrue(decodesBlockByBlock(allocator, commons, small));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies lz4-java frames with content size, content checksum or 64 KiB blocks decode block
     * by block, frames with block checksums and concatenated frames through the frame stream,
     * all to the same bytes.
     *
     * @throws IOException never, the streams are in memory
     */
    @Test
    void framesOfOtherWritersDecode() throws IOException {
        byte[] data = compressible(300 * 1024 + 5);
        CompressionCodec codec = new Lz4FrameCodec();
        try (BufferAllocator allocator = new RootAllocator()) {
            byte[] sized = lz4Java(data, LZ4FrameOutputStream.BLOCKSIZE.SIZE_256KB, data.length,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                    LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE,
                    LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM);
            byte[] small = lz4Java(data, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, -1,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
            byte[] blockChecksums = lz4Java(data, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, -1,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_CHECKSUM);
            byte[] half = new byte[data.length / 2];
            System.arraycopy(data, 0, half, 0, half.length);
            byte[] rest = new byte[data.length - half.length];
            System.arraycopy(data, half.length, rest, 0, rest.length);
            byte[] first = lz4Java(half, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, -1,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
            byte[] second = lz4Java(rest, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, -1,
                    LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
            byte[] concatenated = new byte[first.length + second.length];
            System.arraycopy(first, 0, concatenated, 0, first.length);
            System.arraycopy(second, 0, concatenated, first.length, second.length);

            assertTrue(Lz4FrameCodec.decodeBlocks(frameBuffer(sized), ByteBuffer.allocate(data.length), data.length));
            assertTrue(Lz4FrameCodec.decodeBlocks(frameBuffer(small), ByteBuffer.allocate(data.length), data.length));
            assertFalse(Lz4FrameCodec.decodeBlocks(frameBuffer(blockChecksums), ByteBuffer.allocate(data.length),
                    data.length));
            assertFalse(Lz4FrameCodec.decodeBlocks(frameBuffer(concatenated), ByteBuffer.allocate(data.length),
                    data.length));
            for (byte[] frame : new byte[][]{sized, small, blockChecksums, concatenated}) {
                assertArrayEquals(data, decompress(allocator, codec, frame, data.length));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies a wrong content checksum, a wrong content size and a frame holding more or fewer
     * bytes than announced fail instead of decoding to wrong bytes.
     *
     * @throws IOException never, the streams are in memory
     */
    @Test
    void frameNotMatchingItsChecksumOrLengthFails() throws IOException {
        byte[] data = compressible(100 * 1024);
        CompressionCodec codec = new Lz4FrameCodec();
        byte[] checksummed = lz4Java(data, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, data.length,
                LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE,
                LZ4FrameOutputStream.FLG.Bits.CONTENT_SIZE,
                LZ4FrameOutputStream.FLG.Bits.CONTENT_CHECKSUM);
        byte[] badChecksum = checksummed.clone();
        badChecksum[badChecksum.length - 1] ^= 1;
        byte[] plain = lz4Java(data, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB, -1,
                LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
        try (BufferAllocator allocator = new RootAllocator()) {
            assertThrows(IllegalStateException.class, () -> decompress(allocator, codec, badChecksum, data.length));
            assertThrows(IllegalStateException.class,
                    () -> decompress(allocator, codec, checksummed, data.length - 1));
            assertThrows(IllegalStateException.class, () -> decompress(allocator, codec, plain, data.length - 1));
            assertThrows(IllegalStateException.class, () -> decompress(allocator, codec, plain, data.length + 1));
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    /**
     * Verifies the factory offers LZ4_FRAME only and that Arrow's service loader picks it for
     * LZ4_FRAME (this module's classes come first on the class path) and commons-compress for ZSTD.
     */
    @Test
    void factoryOffersLz4FrameThroughTheServiceLoader() {
        Lz4CodecFactory factory = new Lz4CodecFactory();
        assertTrue(factory.createCodec(CompressionUtil.CodecType.LZ4_FRAME) instanceof Lz4FrameCodec);
        assertTrue(factory.createCodec(CompressionUtil.CodecType.LZ4_FRAME, 9) instanceof Lz4FrameCodec);
        assertThrows(IllegalArgumentException.class, () -> factory.createCodec(CompressionUtil.CodecType.ZSTD));
        assertThrows(IllegalArgumentException.class,
                () -> factory.createCodec(CompressionUtil.CodecType.NO_COMPRESSION));
        assertTrue(CompressionCodec.Factory.INSTANCE.createCodec(CompressionUtil.CodecType.LZ4_FRAME)
                instanceof Lz4FrameCodec);
        assertEquals(CompressionUtil.CodecType.ZSTD,
                CompressionCodec.Factory.INSTANCE.createCodec(CompressionUtil.CodecType.ZSTD).getCodecType());
        assertFalse(CompressionCodec.Factory.INSTANCE.createCodec(CompressionUtil.CodecType.ZSTD)
                instanceof Lz4FrameCodec);
    }

    /**
     * Compresses with a codec and tells whether the frame decodes block by block.
     *
     * @param allocator allocator
     * @param compressor codec that writes the frame
     * @param data input bytes
     * @return true when decoded block by block to the same bytes
     */
    private static boolean decodesBlockByBlock(BufferAllocator allocator, CompressionCodec compressor, byte[] data) {
        // compress() takes over the input buffer.
        try (ArrowBuf compressed = compressor.compress(allocator, buffer(allocator, data))) {
            int frameLength = (int) (compressed.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
            ByteBuffer frame = compressed.nioBuffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, frameLength)
                    .order(ByteOrder.LITTLE_ENDIAN);
            ByteBuffer output = ByteBuffer.allocate(data.length);
            boolean blocks = Lz4FrameCodec.decodeBlocks(frame, output, data.length);
            assertArrayEquals(data, output.array());
            return blocks;
        }
    }

    /**
     * Writes bytes as an lz4-java frame.
     *
     * @param data input bytes
     * @param blockSize block maximum
     * @param knownSize content size for the header, or -1
     * @param bits frame flags
     * @return frame bytes
     * @throws IOException never, the stream is in memory
     */
    private static byte[] lz4Java(byte[] data, LZ4FrameOutputStream.BLOCKSIZE blockSize, long knownSize,
            LZ4FrameOutputStream.FLG.Bits... bits) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (OutputStream out = new LZ4FrameOutputStream(bytes, blockSize, knownSize, bits)) {
            out.write(data);
        }
        return bytes.toByteArray();
    }

    /**
     * Wraps frame bytes as the codec sees them.
     *
     * @param frame frame bytes
     * @return little-endian buffer at position 0
     */
    private static ByteBuffer frameBuffer(byte[] frame) {
        return ByteBuffer.wrap(frame).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * Decompresses a raw frame announced as a given length, as an Arrow reader would.
     *
     * @param allocator allocator
     * @param codec codec
     * @param frame frame bytes
     * @param length announced uncompressed length
     * @return decompressed bytes
     */
    private static byte[] decompress(BufferAllocator allocator, CompressionCodec codec, byte[] frame, int length) {
        ArrowBuf compressed = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + frame.length);
        compressed.setLong(0, length);
        compressed.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, frame);
        compressed.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + frame.length);
        try (ArrowBuf output = codec.decompress(allocator, compressed)) {
            byte[] bytes = new byte[(int) output.writerIndex()];
            output.getBytes(0, bytes);
            return bytes;
        } finally {
            int refs = compressed.getReferenceManager().getRefCount();
            if (refs > 0) {
                compressed.getReferenceManager().release(refs);
            }
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
