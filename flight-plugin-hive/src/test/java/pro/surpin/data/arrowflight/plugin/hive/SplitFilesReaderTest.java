package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.TableFile;

/**
 * Large text and Avro files read by a table scan ({@link TableFilesReader}) as byte ranges
 * decoded ahead on other threads: every row once and in file order whatever the range size and
 * read-ahead, a compressed text file and a text table with header lines as one range.
 */
@Tag("unit")
class SplitFilesReaderTest {

    private static final int ROWS = 40_000;
    private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"t\",\"fields\":[{\"name\":"
            + "\"i\",\"type\":\"int\"},{\"name\":\"s\",\"type\":[\"null\",\"string\"]}]}";
    private static final List<OrcArrowReader.Column> COLUMNS = List.of(
            new OrcArrowReader.Column("i", "int"), new OrcArrowReader.Column("s", "string"));

    @TempDir
    Path dir;

    private BufferAllocator allocator;
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        allocator.close();
    }

    @Test
    void largeTextFilesAreReadAsRangesInOrder() throws Exception {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            text.append(i).append('\u0001').append("row ").append(i)
                    .append(i % 3 == 0 ? "\r\n" : "\n");
        }
        byte[] bytes = text.toString().getBytes(StandardCharsets.UTF_8);
        Files.write(dir.resolve("big.txt"), bytes);
        try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(dir.resolve("big.txt.gz")))) {
            out.write(bytes);
        }
        TableFilesReader.Format plain = TableFilesReader.text(COLUMNS,
                TextFormat.of(Map.of(), Map.of()), 1000);
        checkRanges("big.txt", plain, true);
        checkRanges("big.txt.gz", plain, false);
        Files.write(dir.resolve("header.txt"), ("i\u0001s\n" + text).getBytes(StandardCharsets.UTF_8));
        checkRanges("header.txt", TableFilesReader.text(COLUMNS,
                TextFormat.of(Map.of(), Map.of("skip.header.line.count", "1")), 1000), false);
    }

    @Test
    void largeAvroFilesAreReadAsRangesInOrder() throws Exception {
        Schema schema = AvroFormat.parse(SCHEMA);
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(
                new GenericDatumWriter<>(schema))) {
            writer.setCodec(CodecFactory.deflateCodec(1));
            writer.setSyncInterval(4096);
            writer.create(schema, dir.resolve("big.avro").toFile());
            GenericRecord row = new GenericData.Record(schema);
            for (int i = 0; i < ROWS; i++) {
                row.put("i", i);
                row.put("s", "row " + i);
                writer.append(row);
            }
        }
        AvroFormat format = AvroFormat.of(Map.of(), Map.of(AvroFormat.LITERAL, SCHEMA),
                AvroFormatTest.columns("i int;s string"));
        checkRanges("big.avro", TableFilesReader.avro(COLUMNS, format, 1000), true);
    }

    /**
     * Reads a file with several range sizes and read-aheads and checks the rows and the reads.
     *
     * @param name file name in {@link #dir}
     * @param format the file's format
     * @param splits whether the file is expected to be read as several ranges
     * @throws Exception when the read fails
     */
    private void checkRanges(String name, TableFilesReader.Format format, boolean splits)
            throws Exception {
        long size = Files.size(dir.resolve(name));
        List<TableFile> one = List.of(new TableFile(name, size, 0L, Set.of(), Map.of(), -1L,
                List.of()));
        for (int readAhead : new int[] {1, 3, 8}) {
            for (long range : new long[] {size / 7, 4096L, size, 1L << 30}) {
                String what = name + ", read-ahead " + readAhead + ", range " + range;
                int[] opens = new int[1];
                int expected = 0;
                try (TableFilesReader reader = reader(one, format, readAhead, range, opens)) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    while (reader.loadNextBatch()) {
                        IntVector keys = (IntVector) root.getVector("i");
                        VarCharVector values = (VarCharVector) root.getVector("s");
                        for (int i = 0; i < root.getRowCount(); i++, expected++) {
                            assertEquals(expected, keys.get(i), what);
                            assertEquals("row " + expected,
                                    new String(values.get(i), StandardCharsets.UTF_8), what);
                        }
                    }
                }
                assertEquals(ROWS, expected, what);
                long ranges = !splits || size <= range / 4 ? 1
                        : 1 + TableFilesReader.evenStarts(size,
                                Math.min(range, size / readAhead)).size();
                // A file of more than a quarter of the range size is opened once more to plan.
                assertEquals(ranges + (size > range / 4 ? 1 : 0), opens[0], what);
                assertEquals(0L, allocator.getAllocatedMemory(), what);
            }
        }
    }

    private TableFilesReader reader(List<TableFile> tableFiles, TableFilesReader.Format format,
            int readAhead, long rangeBytes, int[] opens) {
        TestSupport.LocalStore store = new TestSupport.LocalStore(dir.toString());
        return new TableFilesReader(allocator, tableFiles, file -> {
            synchronized (opens) {
                opens[0]++;
            }
            return store.open(file.location());
        }, COLUMNS, List.of(), List.of("i", "s"), format, executor, readAhead, rangeBytes);
    }
}
