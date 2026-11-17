package pro.surpin.data.arrowflight.server.adapters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * Item 54: the prefetching batch reader hands on every batch of a stream unchanged and in order,
 * passes the reader's failure on, and leaves no buffer behind when it is closed early.
 */
class PrefetchingBatchReaderTest {

    private static final int BATCHES = 40;
    private static final int ROWS = 1000;

    private static final Schema SCHEMA = new Schema(List.of(
            Field.nullable("id", new ArrowType.Int(64, true)),
            Field.nullable("name", ArrowType.Utf8.INSTANCE),
            new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE),
                    List.of(Field.nullable("item", new ArrowType.Int(64, true))))));

    /** Writes BATCHES batches of ROWS rows: id = running row number, name and tags from it. */
    private static byte[] stream(BufferAllocator allocator) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            long id = 0;
            for (int b = 0; b < BATCHES; b++) {
                root.allocateNew();
                BigIntVector ids = (BigIntVector) root.getVector("id");
                VarCharVector names = (VarCharVector) root.getVector("name");
                UnionListWriter tags = ((ListVector) root.getVector("tags")).getWriter();
                for (int r = 0; r < ROWS; r++, id++) {
                    ids.setSafe(r, id);
                    if (id % 7 == 0) {
                        names.setNull(r);
                    } else {
                        names.setSafe(r, ("n" + id).getBytes(StandardCharsets.UTF_8));
                    }
                    tags.setPosition(r);
                    tags.startList();
                    for (long t = 0; t < id % 3; t++) {
                        tags.bigInt().writeBigInt(id + t);
                    }
                    tags.endList();
                }
                root.setRowCount(ROWS);
                writer.writeBatch();
            }
            writer.end();
        }
        return out.toByteArray();
    }

    private static void check(VectorSchemaRoot root, long firstId) {
        assertEquals(ROWS, root.getRowCount());
        BigIntVector ids = (BigIntVector) root.getVector("id");
        VarCharVector names = (VarCharVector) root.getVector("name");
        ListVector tags = (ListVector) root.getVector("tags");
        for (int r = 0; r < ROWS; r++) {
            long id = firstId + r;
            assertEquals(id, ids.get(r));
            if (id % 7 == 0) {
                assertTrue(names.isNull(r));
            } else {
                assertEquals("n" + id, new String(names.get(r), StandardCharsets.UTF_8));
            }
            List<?> list = tags.getObject(r);
            assertEquals(id % 3, list.size());
            for (int t = 0; t < list.size(); t++) {
                assertEquals(id + t, list.get(t));
            }
        }
    }

    @Test
    void handsOnEveryBatchInOrder() throws Exception {
        for (int depth : new int[] {1, 2, 8}) {
            try (BufferAllocator allocator = new RootAllocator()) {
                byte[] bytes = stream(allocator);
                try (ArrowReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator);
                        PrefetchingBatchReader prefetcher = new PrefetchingBatchReader(reader,
                                reader::loadNextBatch, allocator, depth);
                        VectorSchemaRoot target = VectorSchemaRoot.create(
                                reader.getVectorSchemaRoot().getSchema(), allocator)) {
                    int batches = 0;
                    while (prefetcher.next(target)) {
                        check(target, (long) batches * ROWS);
                        batches++;
                    }
                    assertEquals(BATCHES, batches);
                    assertFalse(prefetcher.next(target));
                }
                assertEquals(0, allocator.getAllocatedMemory(), "depth " + depth);
            }
        }
    }

    @Test
    void passesTheReadersFailureOn() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            byte[] bytes = stream(allocator);
            IOException failure = new IOException("DuckDB failed");
            AtomicInteger loads = new AtomicInteger();
            try (ArrowReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator);
                    PrefetchingBatchReader prefetcher = new PrefetchingBatchReader(reader, () -> {
                        if (loads.incrementAndGet() == 3) {
                            throw failure;
                        }
                        return reader.loadNextBatch();
                    }, allocator, 2);
                    VectorSchemaRoot target = VectorSchemaRoot.create(
                            reader.getVectorSchemaRoot().getSchema(), allocator)) {
                assertTrue(prefetcher.next(target));
                check(target, 0);
                assertTrue(prefetcher.next(target));
                check(target, ROWS);
                assertSame(failure, assertThrows(IOException.class, () -> prefetcher.next(target)));
                assertFalse(prefetcher.next(target));
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void closedEarlyReleasesQueuedBatches() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            byte[] bytes = stream(allocator);
            AtomicInteger loads = new AtomicInteger();
            try (ArrowReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
                try (PrefetchingBatchReader prefetcher = new PrefetchingBatchReader(reader, () -> {
                    loads.incrementAndGet();
                    return reader.loadNextBatch();
                }, allocator, 4);
                        VectorSchemaRoot target = VectorSchemaRoot.create(
                                reader.getVectorSchemaRoot().getSchema(), allocator)) {
                    assertTrue(prefetcher.next(target));
                    check(target, 0);
                    long deadline = System.nanoTime() + 5_000_000_000L;
                    while (loads.get() < 6 && System.nanoTime() < deadline) {
                        Thread.sleep(5);
                    }
                }
                // At most the queue and the batch in hand were read ahead, not the whole stream.
                assertTrue(loads.get() <= 7, "loads " + loads.get());
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    private static ConfigSource config(java.util.Map<String, String> system) {
        return new ConfigSource(system::get, new java.util.Properties(), java.util.Map.of(), null);
    }

    @Test
    void settingDefaultsToOff() {
        assertEquals(0, ConfigAdapter.flightStreamPrefetchBatches(config(java.util.Map.of())));
        assertEquals(2, ConfigAdapter.flightStreamPrefetchBatches(
                config(java.util.Map.of("flightStreamPrefetchBatches", "2"))));
        assertThrows(IllegalArgumentException.class, () -> ConfigAdapter.flightStreamPrefetchBatches(
                config(java.util.Map.of("flightStreamPrefetchBatches", "-1"))));
    }
}
