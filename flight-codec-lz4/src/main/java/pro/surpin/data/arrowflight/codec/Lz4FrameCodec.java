package pro.surpin.data.arrowflight.codec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4SafeDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * Arrow LZ4_FRAME codec on lz4-java, which the runtime libs set carries for Hadoop's Lz4Codec:
 * writes standard LZ4 frames of independent 4 MiB blocks without checksums straight from the
 * Arrow buffer into the output buffer, and reads any LZ4 frame: frames of independent blocks
 * without block checksums (its own, commons-compress's) block by block straight into the output
 * buffer, others through lz4-java's frame stream. Arrow Java's own LZ4 codec (commons-compress)
 * compresses at well under 1 MB/s and decompresses at about 180 MB/s.
 */
public final class Lz4FrameCodec extends AbstractCompressionCodec {

    private static final String PROBE = "net.jpountz.lz4.LZ4FrameOutputStream";
    /** Block maximum of the frame header (block maximum size id 7). */
    private static final int BLOCK_BYTES = 4 << 20;
    private static final int BLOCK_SIZE_BYTES = 4;
    private static final int END_MARK_BYTES = 4;
    private static final int UNCOMPRESSED_BLOCK = 0x80000000;
    private static final int MAGIC = 0x184D2204;
    private static final int MAGIC_BYTES = 4;
    private static final int CHECKSUM_BYTES = 4;
    private static final int CONTENT_SIZE_BYTES = 8;
    private static final int VERSION_MASK = 0xC0;
    private static final int VERSION_1 = 0x40;
    private static final int FLAG_INDEPENDENT_BLOCKS = 0x20;
    private static final int FLAG_BLOCK_CHECKSUM = 0x10;
    private static final int FLAG_CONTENT_SIZE = 0x08;
    private static final int FLAG_CONTENT_CHECKSUM = 0x04;
    private static final int FLAG_RESERVED_OR_DICTIONARY = 0x03;
    private static final int BLOCK_SIZE_ID_SHIFT = 4;
    private static final int BLOCK_SIZE_ID_MASK = 0x07;
    private static final int BLOCK_SIZE_ID_MIN = 4;
    private static final byte[] HEADER = available() ? header() : new byte[0];

    /**
     * Tells whether lz4-java is on the class path.
     *
     * @return true when the codec can be used
     */
    public static boolean available() {
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
        int frameLength = checkedLength(compressedBuffer.writerIndex()
                - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
        ByteBuffer frame = compressedBuffer.nioBuffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, frameLength)
                .order(ByteOrder.LITTLE_ENDIAN);
        ArrowBuf decompressed = allocator.buffer(length);
        try {
            ByteBuffer output = decompressed.nioBuffer(0, length);
            if (!decodeBlocks(frame, output, length)) {
                decodeStream(frame, output, length);
            }
            decompressed.writerIndex(length);
            return decompressed;
        } catch (RuntimeException e) {
            decompressed.close();
            throw e;
        }
    }

    /**
     * Decodes a frame of independent blocks without block checksums or dictionary block by block
     * straight into the output; any other frame is left to the stream decoder.
     *
     * @param frame frame bytes, little-endian, position 0
     * @param output output buffer of exactly the announced length
     * @param length announced uncompressed length
     * @return true when decoded, false when the frame needs the stream decoder
     * @throws IllegalStateException if the frame is corrupt or does not hold the announced bytes
     */
    static boolean decodeBlocks(ByteBuffer frame, ByteBuffer output, int length) {
        int end = frame.limit();
        if (end < MAGIC_BYTES + 3 || frame.getInt(0) != MAGIC) {
            return false;
        }
        int flags = frame.get(MAGIC_BYTES) & 0xFF;
        int blockDescriptor = frame.get(MAGIC_BYTES + 1) & 0xFF;
        if ((flags & VERSION_MASK) != VERSION_1 || (flags & FLAG_INDEPENDENT_BLOCKS) == 0
                || (flags & (FLAG_BLOCK_CHECKSUM | FLAG_RESERVED_OR_DICTIONARY)) != 0
                || (blockDescriptor & ~(BLOCK_SIZE_ID_MASK << BLOCK_SIZE_ID_SHIFT)) != 0) {
            return false;
        }
        int sizeId = (blockDescriptor >> BLOCK_SIZE_ID_SHIFT) & BLOCK_SIZE_ID_MASK;
        if (sizeId < BLOCK_SIZE_ID_MIN) {
            return false;
        }
        int blockMax = 1 << (8 + 2 * sizeId);
        int descriptorEnd = MAGIC_BYTES + 2 + ((flags & FLAG_CONTENT_SIZE) != 0 ? CONTENT_SIZE_BYTES : 0);
        if (end < descriptorEnd + 1) {
            return false;
        }
        int check = XXHashFactory.fastestInstance().hash32().hash(frame, MAGIC_BYTES,
                descriptorEnd - MAGIC_BYTES, 0);
        if ((byte) ((check >> 8) & 0xFF) != frame.get(descriptorEnd)) {
            throw new IllegalStateException("LZ4 frame header checksum does not match");
        }
        if ((flags & FLAG_CONTENT_SIZE) != 0 && frame.getLong(MAGIC_BYTES + 2) != length) {
            throw new IllegalStateException("LZ4 frame content size " + frame.getLong(MAGIC_BYTES + 2)
                    + " differs from the announced " + length + " bytes");
        }
        LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();
        int pos = descriptorEnd + 1;
        int written = 0;
        while (true) {
            if (end - pos < BLOCK_SIZE_BYTES) {
                throw new IllegalStateException("LZ4 frame ends inside a block header");
            }
            int header = frame.getInt(pos);
            pos += BLOCK_SIZE_BYTES;
            if (header == 0) {
                break;
            }
            int size = header & ~UNCOMPRESSED_BLOCK;
            if (size > blockMax || size > end - pos) {
                throw new IllegalStateException("LZ4 frame block of " + size + " bytes is corrupt");
            }
            int room = Math.min(blockMax, length - written);
            if ((header & UNCOMPRESSED_BLOCK) != 0) {
                if (size > room) {
                    throw new IllegalStateException("LZ4 frame holds more than the announced " + length + " bytes");
                }
                output.put(written, frame, pos, size);
                written += size;
            } else {
                try {
                    written += decompressor.decompress(frame, pos, size, output, written, room);
                } catch (LZ4Exception e) {
                    throw new IllegalStateException("LZ4 frame block is corrupt or holds more than the announced "
                            + length + " bytes", e);
                }
            }
            pos += size;
        }
        if ((flags & FLAG_CONTENT_CHECKSUM) != 0) {
            if (end - pos < CHECKSUM_BYTES) {
                throw new IllegalStateException("LZ4 frame ends before its content checksum");
            }
            if (XXHashFactory.fastestInstance().hash32().hash(output, 0, written, 0) != frame.getInt(pos)) {
                throw new IllegalStateException("LZ4 frame content checksum does not match");
            }
            pos += CHECKSUM_BYTES;
        }
        if (pos != end) {
            // Further frames follow: the stream decoder reads them all.
            return false;
        }
        if (written != length) {
            throw new IllegalStateException("LZ4 frame does not hold the " + length + " bytes it announces");
        }
        return true;
    }

    /**
     * Decodes any LZ4 frame sequence through lz4-java's frame stream.
     *
     * @param frame frame bytes, position 0
     * @param output output buffer of exactly the announced length
     * @param length announced uncompressed length
     * @throws IllegalStateException if the frames do not hold the announced bytes
     * @throws UncheckedIOException if the frames are corrupt
     */
    private static void decodeStream(ByteBuffer frame, ByteBuffer output, int length) {
        byte[] input = new byte[frame.limit()];
        frame.get(0, input);
        byte[] bytes = new byte[length];
        try (InputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(input))) {
            if (in.readNBytes(bytes, 0, length) != length || in.read() != -1) {
                throw new IllegalStateException("LZ4 frame does not hold the " + length + " bytes it announces");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("LZ4 frame decompression failed", e);
        }
        output.put(0, bytes);
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
