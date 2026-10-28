package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.TableFile;

/**
 * The stream of an ORC table scan ({@link OrcTableReader}) with files decoded ahead on other
 * threads: batches arrive in file order whatever the read-ahead, closing early frees every
 * buffer and stops the decoding, and a broken file fails the stream after the rows of the files
 * before it.
 */
@Tag("unit")
class OrcTableReaderTest {

    private static final int FILES = 6;
    private static final int ROWS_PER_FILE = 10_000;

    @TempDir
    Path dir;

    private BufferAllocator allocator;
    private ExecutorService executor;
    private final List<TableFile> files = new ArrayList<>();

    @BeforeEach
    void setUp() throws IOException {
        allocator = new RootAllocator();
        executor = Executors.newCachedThreadPool();
        for (int f = 0; f < FILES; f++) {
            Path file = dir.resolve("f" + f + ".orc");
            OrcTestFiles.writeLineitem(file.toString(), (long) f * ROWS_PER_FILE,
                    (long) (f + 1) * ROWS_PER_FILE, "ZSTD");
            files.add(new TableFile(file.getFileName().toString(), Files.size(file), 0L, Set.of(),
                    Map.of("part", "p" + f), -1L, List.of()));
        }
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        allocator.close();
    }

    @Test
    void batchesArriveInFileOrderWhateverTheReadAhead() throws Exception {
        for (int readAhead : new int[] {1, 3, FILES + 2}) {
            long expected = 0;
            try (OrcTableReader reader = reader(files, readAhead)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                while (reader.loadNextBatch()) {
                    BigIntVector keys = (BigIntVector) root.getVector("l_orderkey");
                    VarCharVector parts = (VarCharVector) root.getVector("part");
                    for (int i = 0; i < root.getRowCount(); i++, expected++) {
                        // l_orderkey = row / 4, so the rows' order shows in the keys.
                        assertEquals(expected / 4, keys.get(i), "read-ahead " + readAhead);
                        assertEquals("p" + expected / ROWS_PER_FILE,
                                new String(parts.get(i), StandardCharsets.UTF_8));
                    }
                }
            }
            assertEquals((long) FILES * ROWS_PER_FILE, expected, "read-ahead " + readAhead);
            assertEquals(0L, allocator.getAllocatedMemory());
        }
    }

    @Test
    void closingEarlyFreesEveryBatchDecodedAhead() throws Exception {
        try (OrcTableReader reader = reader(files, FILES)) {
            assertTrue(reader.loadNextBatch());
            // Give the other files time to fill their queues.
            Thread.sleep(300);
        }
        assertEquals(0L, allocator.getAllocatedMemory());
    }

    @Test
    void aBrokenFileFailsAfterTheRowsBeforeIt() throws Exception {
        Files.write(dir.resolve("broken.orc"), "not an ORC file".getBytes(StandardCharsets.UTF_8));
        List<TableFile> withBroken = new ArrayList<>(files.subList(0, 2));
        withBroken.add(new TableFile("broken.orc", 15L, 0L, Set.of(), Map.of(), -1L, List.of()));
        withBroken.addAll(files.subList(2, FILES));
        long[] read = new long[1];
        try (OrcTableReader reader = reader(withBroken, 4)) {
            IOException e = assertThrows(IOException.class, () -> {
                while (reader.loadNextBatch()) {
                    read[0] += reader.getVectorSchemaRoot().getRowCount();
                }
            });
            assertTrue(e.getMessage().contains("broken.orc"), e.getMessage());
        }
        assertEquals(2L * ROWS_PER_FILE, read[0]);
        assertEquals(0L, allocator.getAllocatedMemory());
    }

    @Test
    void aRejectedReadFailsTheStreamAndClosesAtOnce() throws Exception {
        executor.shutdown();
        try (OrcTableReader reader = reader(files, 2)) {
            assertThrows(java.util.concurrent.RejectedExecutionException.class,
                    reader::loadNextBatch);
        }
        assertEquals(0L, allocator.getAllocatedMemory());
    }

    private OrcTableReader reader(List<TableFile> tableFiles, int readAhead) {
        TestSupport.LocalStore store = new TestSupport.LocalStore(dir.toString());
        return new OrcTableReader(allocator, tableFiles, file -> store.open(file.location()),
                List.of(new OrcArrowReader.Column("l_orderkey", "bigint"),
                        new OrcArrowReader.Column("l_quantity", "decimal(15,2)"),
                        new OrcArrowReader.Column("l_comment", "string")),
                List.of("part"), List.of("l_orderkey", "part", "l_comment"), null, 1000,
                executor, readAhead);
    }
}
