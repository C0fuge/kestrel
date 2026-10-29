package pro.surpin.data.arrowflight.codec;

import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * Arrow codec factory for LZ4_FRAME on lz4-java, registered as a service provider so that Arrow
 * Java readers (Flight clients, IPC readers) decode LZ4 streams with {@link Lz4FrameCodec}. It
 * offers no other codec type, and none when lz4-java is missing, so Arrow then falls back to
 * the next registered factory. Arrow takes the first factory per type in class path order:
 * put this jar before arrow-compression for LZ4 to use it.
 */
public final class Lz4CodecFactory implements CompressionCodec.Factory {

    /**
     * Creates the factory; Arrow's service loader calls this.
     */
    public Lz4CodecFactory() {
        // Stateless.
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
        if (codecType != CompressionUtil.CodecType.LZ4_FRAME) {
            throw new IllegalArgumentException("Only LZ4_FRAME is offered here, not " + codecType);
        }
        if (!Lz4FrameCodec.available()) {
            throw new IllegalStateException("lz4-java is not on the class path");
        }
        return new Lz4FrameCodec();
    }

    @Override
    public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
        return createCodec(codecType);
    }
}
