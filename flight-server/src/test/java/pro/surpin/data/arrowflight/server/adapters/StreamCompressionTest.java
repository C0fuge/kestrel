package pro.surpin.data.arrowflight.server.adapters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests codec choice per call, the compression settings, and the unloader's plain and give-up
 * rules.
 */
@Tag("unit")
class StreamCompressionTest {

    /**
     * Verifies the codec names clients may send.
     */
    @Test
    void parsesCodecNames() {
        assertEquals(StreamCompression.Codec.ZSTD, StreamCompression.parse(" ZSTD "));
        assertEquals(StreamCompression.Codec.LZ4, StreamCompression.parse("lz4"));
        assertEquals(StreamCompression.Codec.LZ4, StreamCompression.parse("LZ4_FRAME"));
        assertEquals(StreamCompression.Codec.LZ4, StreamCompression.parse("lz4-frame"));
        assertEquals(StreamCompression.Codec.NONE, StreamCompression.parse("none"));
        assertThrows(IllegalArgumentException.class, () -> StreamCompression.parse("gzip"));
    }

    /**
     * Verifies the header wins over the server default, and no header means the default.
     */
    @Test
    void headerChoosesOverTheServerDefault() {
        StreamCompression.Factory plain = new StreamCompression.Factory(StreamCompression.Settings.DEFAULT);
        StreamCompression.Factory zstd = new StreamCompression.Factory(
                new StreamCompression.Settings(StreamCompression.Codec.ZSTD, 3, 0));
        assertEquals(StreamCompression.Codec.NONE, call(plain, null).codec());
        assertEquals(StreamCompression.Codec.ZSTD, call(plain, "zstd").codec());
        assertEquals(StreamCompression.Codec.ZSTD, call(zstd, null).codec());
        assertEquals(StreamCompression.Codec.NONE, call(zstd, "none").codec());

        FlightProducer.ServerStreamListener listener = Mockito.mock(FlightProducer.ServerStreamListener.class);
        assertSame(listener, call(plain, null).wrap(listener));
        assertInstanceOf(CompressingStreamListener.class, call(plain, "zstd").wrap(listener));
        FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                () -> call(plain, "brotli").wrap(listener));
        assertEquals(CallStatus.INVALID_ARGUMENT.code(), e.status().code());
        assertEquals(StreamCompression.Codec.LZ4, call(zstd, "lz4").codec());
        assertInstanceOf(CompressingStreamListener.class, call(plain, "lz4").wrap(listener));
    }

    /**
     * Verifies a session's codec goes before the server default but after the call header.
     */
    @Test
    void sessionCodecGoesBetweenHeaderAndDefault() {
        StreamCompression.Factory zstd = new StreamCompression.Factory(
                new StreamCompression.Settings(StreamCompression.Codec.ZSTD, 3, 0));
        FlightProducer.ServerStreamListener listener = Mockito.mock(FlightProducer.ServerStreamListener.class);
        assertSame(listener, call(zstd, null).wrap(listener, StreamCompression.Codec.NONE));
        assertInstanceOf(CompressingStreamListener.class, call(zstd, null).wrap(listener, null));
        StreamCompression.Factory plain = new StreamCompression.Factory(StreamCompression.Settings.DEFAULT);
        assertInstanceOf(CompressingStreamListener.class,
                call(plain, null).wrap(listener, StreamCompression.Codec.LZ4));
        assertSame(listener, call(plain, "none").wrap(listener, StreamCompression.Codec.ZSTD));
        assertInstanceOf(CompressingStreamListener.class,
                call(plain, "zstd").wrap(listener, StreamCompression.Codec.NONE));
    }

    /**
     * Verifies the session store settings are read and checked.
     */
    @Test
    void readsSessionSettings() {
        assertEquals(0, ConfigAdapter.flightSessions(source(Map.of())).size());
        ConfigAdapter.flightSessions(source(Map.of("flightSessionIdleSeconds", "5", "flightSessionMax", "1")));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.flightSessions(source(Map.of("flightSessionMax", "0"))));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.flightSessions(source(Map.of("flightSessionIdleSeconds", "-1"))));
    }

    /**
     * Verifies the settings are read from the configuration and checked.
     */
    @Test
    void readsAndChecksSettings() {
        StreamCompression.Settings defaults = ConfigAdapter.streamCompression(source(Map.of()));
        assertEquals(StreamCompression.Settings.DEFAULT, defaults);
        StreamCompression.Settings set = ConfigAdapter.streamCompression(source(Map.of(
                "flightCompression", "zstd", "arrowflight.flight.compressionLevel", "5",
                "flightCompressionMinBytes", "0")));
        assertEquals(new StreamCompression.Settings(StreamCompression.Codec.ZSTD, 5, 0), set);
        assertEquals(StreamCompression.Codec.LZ4, ConfigAdapter.streamCompression(
                source(Map.of("flightCompression", "lz4"))).defaultCodec());
        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.streamCompression(source(Map.of("flightCompression", "snappy"))));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.streamCompression(source(Map.of("flightCompressionLevel", "0"))));
        assertThrows(IllegalArgumentException.class,
                () -> ConfigAdapter.streamCompression(source(Map.of("flightCompressionMinBytes", "-1"))));
    }

    /**
     * Verifies large compressible batches are compressed and decode to the same values, and
     * small ones go plain.
     */
    @Test
    void compressesLargeBatchesOnly() {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.of(new BigIntVector("id", allocator));
                VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator)) {
            for (StreamCompression.Codec codec : new StreamCompression.Codec[]{
                    StreamCompression.Codec.ZSTD, StreamCompression.Codec.LZ4}) {
                CompressingStreamListener.Unloader unloader = new CompressingStreamListener.Unloader(root,
                        CompressingStreamListener.arrowCodec(codec, 1), 64 * 1024);
                fill(root, 50_000);
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    assertTrue(batch.getBodyCompression().getCodec() != CompressionUtil.CodecType.NO_COMPRESSION.getType(),
                            codec.name());
                    assertTrue(batch.computeBodyLength() < CompressingStreamListener.Unloader.bodyBytes(root) / 2,
                            codec.name());
                    try (ArrowRecordBatch read = ipcRoundTrip(batch, allocator)) {
                        new VectorLoader(copy, CommonsCompressionFactory.INSTANCE).load(read);
                    }
                }
                assertEquals(root.contentToTSVString(), copy.contentToTSVString(), codec.name());
                fill(root, 100);
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    assertEquals(CompressionUtil.CodecType.NO_COMPRESSION.getType(),
                            batch.getBodyCompression().getCodec(), codec.name());
                }
            }
        }
    }

    /**
     * Verifies a stream of incompressible batches stops compressing after the ratio check.
     */
    @Test
    void givesUpWhenBatchesDoNotShrink() {
        Random random = new Random(42);
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.of(new VarBinaryVector("b", allocator))) {
            CompressingStreamListener.Unloader unloader = new CompressingStreamListener.Unloader(root,
                    CompressingStreamListener.arrowCodec(StreamCompression.Codec.ZSTD, 1), 0);
            VarBinaryVector vector = (VarBinaryVector) root.getVector(0);
            for (int i = 0; i < CompressingStreamListener.RATIO_CHECK_BATCHES + 1; i++) {
                assertFalse(unloader.givenUp() && i < CompressingStreamListener.RATIO_CHECK_BATCHES);
                vector.allocateNew();
                byte[] value = new byte[256];
                for (int row = 0; row < 1000; row++) {
                    random.nextBytes(value);
                    vector.setSafe(row, value);
                }
                root.setRowCount(1000);
                try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
                    byte expected = i < CompressingStreamListener.RATIO_CHECK_BATCHES
                            ? CompressionUtil.CodecType.ZSTD.getType()
                            : CompressionUtil.CodecType.NO_COMPRESSION.getType();
                    assertEquals(expected, batch.getBodyCompression().getCodec(), "batch " + i);
                }
            }
            assertTrue(unloader.givenUp());
        }
    }

    /**
     * Writes a batch as an IPC message and reads it back, as a client receives it.
     *
     * @param batch batch to send
     * @param allocator allocator of the read batch
     * @return batch read back
     */
    private static ArrowRecordBatch ipcRoundTrip(ArrowRecordBatch batch, BufferAllocator allocator) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);
            return MessageSerializer.deserializeRecordBatch(new ReadChannel(Channels.newChannel(
                    new ByteArrayInputStream(out.toByteArray()))), allocator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Fills a one-column root with ids.
     *
     * @param root root with one BIGINT column
     * @param rows row count
     */
    private static void fill(VectorSchemaRoot root, int rows) {
        BigIntVector vector = (BigIntVector) root.getVector(0);
        vector.allocateNew(rows);
        for (int i = 0; i < rows; i++) {
            vector.set(i, i % 100);
        }
        root.setRowCount(rows);
    }

    /**
     * Starts the middleware of one call.
     *
     * @param factory middleware factory
     * @param header compression header value, or null
     * @return middleware
     */
    private static StreamCompression call(StreamCompression.Factory factory, String header) {
        FlightCallHeaders headers = new FlightCallHeaders();
        if (header != null) {
            headers.insert(StreamCompression.HEADER, header);
        }
        return factory.onCallStarted(null, headers, null);
    }

    /**
     * Builds a configuration source of system properties only.
     *
     * @param sys system properties
     * @return source
     */
    private static ConfigSource source(Map<String, String> sys) {
        return new ConfigSource(sys::get, new Properties(), Map.of(), null);
    }
}
