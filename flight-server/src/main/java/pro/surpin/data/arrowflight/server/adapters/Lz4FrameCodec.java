package pro.surpin.data.arrowflight.server.adapters;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * Arrow LZ4_FRAME codec on lz4-java, which the runtime libs set carries for Hadoop's Lz4Codec:
 * writes standard LZ4 frames of independent 4 MiB blocks without checksums straight from the
 * Arrow buffer into the output buffer, and reads any LZ4 frame. Arrow Java's own LZ4 codec
 * (commons-compress) compresses at well under 1 MB/s.
 */
final class Lz4FrameCodec extends AbstractCompressionCodec {

    private static final String PROBE = "net.jpountz.lz4.LZ4FrameOutputStream";
    /** Block maximum of the frame header (block maximum size id 7). */
    private static final int BLOCK_BYTES = 4 << 20;
    private static final int BLOCK_SIZE_BYTES = 4;
    private static final int END_MARK_BYTES = 4;
    private static final int UNCOMPRESSED_BLOCK = 0x80000000;
    private static final byte[] HEADER = available() ? header() : new byte[0];

    /**
     * Tells whether lz4-java is on the class path.
     *
     * @return true when the codec can be used
     */
    static boolean available() {
        try {
            Class.forName(PROBE, false, Lz4FrameCodec.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
        int length = checkedLength(uncompressedBuffer.writerIndex());
        LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        int blocks = Math.max(1, (length + BLOCK_BYTES - 1) / BLOCK_BYTES);
        long capacity = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + HEADER.length + END_MARK_BYTES
                + (long) blocks * (BLOCK_SIZE_BYTES + compressor.maxCompressedLength(Math.min(length, BLOCK_BYTES)));
        ArrowBuf compressed = allocator.buffer(checkedLength(capacity));
        try {
            ByteBuffer src = uncompressedBuffer.nioBuffer(0, length);
            ByteBuffer dst = compressed.nioBuffer(0, (int) capacity).order(ByteOrder.LITTLE_ENDIAN);
            int pos = (int) CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
            dst.put(pos, HEADER);
            pos += HEADER.length;
            for (int off = 0; off < length; off += BLOCK_BYTES) {
                int n = Math.min(BLOCK_BYTES, length - off);
                int c = compressor.compress(src, off, n, dst, pos + BLOCK_SIZE_BYTES,
                        compressor.maxCompressedLength(n));
                if (c < n) {
                    dst.putInt(pos, c);
                    pos += BLOCK_SIZE_BYTES + c;
                } else {
                    dst.putInt(pos, n | UNCOMPRESSED_BLOCK);
                    dst.put(pos + BLOCK_SIZE_BYTES, src, off, n);
                    pos += BLOCK_SIZE_BYTES + n;
                }
            }
            dst.putInt(pos, 0);
            compressed.writerIndex(pos + END_MARK_BYTES);
            return compressed;
        } catch (RuntimeException e) {
            compressed.close();
            throw e;
        }
    }

    /**
     * Builds the frame header: magic, version 1 with independent blocks, 4 MiB block maximum,
     * no checksums or content size, and the header checksum.
     *
     * @return header bytes
     */
    private static byte[] header() {
        byte[] descriptor = {0x60, 0x70};
        int check = XXHashFactory.fastestInstance().hash32().hash(descriptor, 0, descriptor.length, 0);
        return new byte[]{0x04, 0x22, 0x4D, 0x18, descriptor[0], descriptor[1], (byte) ((check >> 8) & 0xFF)};
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
        int length = checkedLength(readUncompressedLength(compressedBuffer));
        byte[] frame = new byte[checkedLength(compressedBuffer.writerIndex()
                - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
        compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, frame);
        byte[] output = new byte[length];
        try (InputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(frame))) {
            if (in.readNBytes(output, 0, length) != length || in.read() != -1) {
                throw new IllegalStateException("LZ4 frame does not hold the " + length + " bytes it announces");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("LZ4 frame decompression failed", e);
        }
        ArrowBuf decompressed = allocator.buffer(length);
        decompressed.setBytes(0, output);
        decompressed.writerIndex(length);
        return decompressed;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.LZ4_FRAME;
    }

    /**
     * Checks that a buffer fits a Java array.
     *
     * @param length buffer length
     * @return length as int
     * @throws IllegalArgumentException if the buffer is larger than 2 GiB
     */
    private static int checkedLength(long length) {
        if (length < 0 || length > Integer.MAX_VALUE - 64) {
            throw new IllegalArgumentException("Buffer too large for LZ4 frames: " + length + " bytes");
        }
        return (int) length;
    }
}
