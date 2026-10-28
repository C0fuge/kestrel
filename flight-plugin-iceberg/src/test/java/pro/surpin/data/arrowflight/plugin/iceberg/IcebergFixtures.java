package pro.surpin.data.arrowflight.plugin.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/**
 * Writes the Iceberg test tables with Iceberg Java into a warehouse directory in the Hadoop
 * layout (HadoopCatalog), namespace {@code ice}. Used by this module's tests and, through a
 * class loader over target/fixture-libs, by the server's catalog tests; so its entry points take
 * and return JDK types only.
 *
 * <p>Tables and their expected content:
 * <ul>
 * <li>types: every primitive type, struct, list and map; 2 data files, 5 rows (ids 1-5), no deletes</li>
 * <li>evolved: id int -> long, name renamed to label, amount decimal(10,2) -> (12,2), column note
 *     added; 2 snapshots, 7 rows (ids 1-7; note only for 5-7)</li>
 * <li>parted: identity(region) + day(ts) partitions, 3 files, 6 rows</li>
 * <li>deletes: v2; 5 rows written, id 2 removed by a position delete, id 4 by an equality delete,
 *     name renamed to label, then ids 6, 7: rows 1,3,5,6,7</li>
 * <li>nested: struct field st.b renamed to st.c between two files; 4 rows</li>
 * <li>empty: no snapshot</li>
 * </ul>
 */
public final class IcebergFixtures {

    /** Namespace of every test table. */
    public static final String NAMESPACE = "ice";

    private static final AtomicInteger FILES = new AtomicInteger();

    private IcebergFixtures() {
    }

    /**
     * Writes all test tables.
     *
     * @param warehouse warehouse directory (created when missing)
     * @return table name to its current metadata file
     * @throws IOException when a file cannot be written
     */
    public static Map<String, String> write(String warehouse) throws IOException {
        HadoopCatalog catalog = catalog(warehouse);
        try {
            return write(catalog);
        } finally {
            catalog.close();
        }
    }

    /**
     * Writes all test tables through any Iceberg catalog (a REST catalog in the conformance
     * tests) whose namespace {@link #NAMESPACE} exists.
     *
     * @param catalog catalog
     * @return table name to its current metadata file
     * @throws IOException when a file cannot be written
     */
    static Map<String, String> write(Catalog catalog) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        result.put("types", metadata(types(catalog)));
        result.put("evolved", metadata(evolved(catalog)));
        result.put("parted", metadata(parted(catalog)));
        result.put("deletes", metadata(deletes(catalog)));
        result.put("nested", metadata(nested(catalog)));
        result.put("empty", metadata(catalog.createTable(id("empty"), new Schema(
                required(1, "id", Types.LongType.get())), PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"))));
        return result;
    }

    /**
     * Appends rows to the events-like table {@code evolved}: a new snapshot for the tests of
     * snapshot pinning.
     *
     * @param warehouse warehouse directory
     * @param firstId first id of the two new rows
     * @return new metadata file
     * @throws IOException when a file cannot be written
     */
    public static String appendToEvolved(String warehouse, long firstId) throws IOException {
        HadoopCatalog catalog = catalog(warehouse);
        try {
            Table t = catalog.loadTable(id("evolved"));
            Schema s = t.schema();
            List<Record> rows = new ArrayList<>();
            for (long id = firstId; id < firstId + 2; id++) {
                rows.add(row(s, "id", id, "label", "l" + id, "amount", new BigDecimal("1.50"),
                        "note", "n" + id));
            }
            t.newAppend().appendFile(writeData(t, rows, null)).commit();
            return metadata(t);
        } finally {
            catalog.close();
        }
    }

    /**
     * Opens the Hadoop catalog of a warehouse.
     *
     * @param warehouse directory
     * @return catalog
     */
    static HadoopCatalog catalog(String warehouse) {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize("fixtures", Map.of("warehouse", warehouse));
        if (!catalog.namespaceExists(Namespace.of(NAMESPACE))) {
            catalog.createNamespace(Namespace.of(NAMESPACE));
        }
        return catalog;
    }

    private static TableIdentifier id(String table) {
        return TableIdentifier.of(NAMESPACE, table);
    }

    private static String metadata(Table table) {
        return ((HasTableOperations) table).operations().current().metadataFileLocation();
    }

    private static Table types(Catalog catalog) throws IOException {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "b", Types.BooleanType.get()),
                optional(3, "i", Types.IntegerType.get()),
                optional(4, "f", Types.FloatType.get()),
                optional(5, "d", Types.DoubleType.get()),
                optional(6, "dec", Types.DecimalType.of(12, 2)),
                optional(7, "dec38", Types.DecimalType.of(38, 6)),
                optional(8, "dt", Types.DateType.get()),
                optional(9, "tm", Types.TimeType.get()),
                optional(10, "ts", Types.TimestampType.withoutZone()),
                optional(11, "tstz", Types.TimestampType.withZone()),
                optional(12, "s", Types.StringType.get()),
                optional(13, "u", Types.UUIDType.get()),
                optional(14, "bin", Types.BinaryType.get()),
                optional(15, "fx", Types.FixedType.ofLength(4)),
                optional(16, "st", Types.StructType.of(
                        optional(20, "a", Types.IntegerType.get()),
                        optional(21, "b", Types.StringType.get()))),
                optional(17, "lst", Types.ListType.ofOptional(22, Types.IntegerType.get())),
                optional(18, "mp", Types.MapType.ofOptional(23, 24, Types.StringType.get(),
                        Types.IntegerType.get())));
        Table t = catalog.createTable(id("types"), schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        List<Record> first = new ArrayList<>();
        List<Record> second = new ArrayList<>();
        for (int n = 1; n <= 5; n++) {
            Record r = GenericRecord.create(schema);
            r.setField("id", (long) n);
            if (n != 3) {
                r.setField("b", n % 2 == 0);
                r.setField("i", n * 10);
                r.setField("f", n + 0.5f);
                r.setField("d", n * 1.25d);
                r.setField("dec", new BigDecimal(n + "123.45"));
                r.setField("dec38", new BigDecimal("12345678901234567890123456789012." + n + "00000"));
                r.setField("dt", LocalDate.of(2024, 1, n));
                r.setField("tm", LocalTime.of(10, n, 30));
                r.setField("ts", LocalDateTime.of(2024, 1, n, 12, 0, 1, 123456000));
                r.setField("tstz", OffsetDateTime.of(2024, 1, n, 12, 0, 0, 0, ZoneOffset.UTC));
                r.setField("s", "text " + n + (n == 5 ? " äöü" : ""));
                r.setField("u", UUID.nameUUIDFromBytes(("u" + n).getBytes(StandardCharsets.UTF_8)));
                r.setField("bin", ByteBuffer.wrap(new byte[] {(byte) n, 0, (byte) 255}));
                r.setField("fx", new byte[] {1, 2, 3, (byte) n});
                Record st = GenericRecord.create(schema.findField("st").type().asStructType());
                st.setField("a", n);
                st.setField("b", "b" + n);
                r.setField("st", st);
                r.setField("lst", List.of(n, n + 1));
                r.setField("mp", Map.of("k" + n, n));
            }
            (n <= 3 ? first : second).add(r);
        }
        t.newAppend().appendFile(writeData(t, first, null)).commit();
        t.newAppend().appendFile(writeData(t, second, null)).commit();
        return t;
    }

    private static Table evolved(Catalog catalog) throws IOException {
        Schema v1 = new Schema(
                required(1, "id", Types.IntegerType.get()),
                optional(2, "name", Types.StringType.get()),
                optional(3, "amount", Types.DecimalType.of(10, 2)));
        Table t = catalog.createTable(id("evolved"), v1, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        List<Record> rows = new ArrayList<>();
        for (int n = 1; n <= 4; n++) {
            rows.add(row(v1, "id", n, "name", "n" + n, "amount", new BigDecimal(n + ".25")));
        }
        t.newAppend().appendFile(writeData(t, rows, null)).commit();
        t.updateSchema().renameColumn("name", "label")
                .updateColumn("id", Types.LongType.get())
                .updateColumn("amount", Types.DecimalType.of(12, 2))
                .addColumn("note", Types.StringType.get()).commit();
        t.refresh();
        Schema v2 = t.schema();
        rows.clear();
        for (long n = 5; n <= 7; n++) {
            rows.add(row(v2, "id", n, "label", "l" + n, "amount", new BigDecimal("1000000" + n + ".75"),
                    "note", "note " + n));
        }
        t.newAppend().appendFile(writeData(t, rows, null)).commit();
        return t;
    }

    private static Table parted(Catalog catalog) throws IOException {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "region", Types.StringType.get()),
                optional(3, "ts", Types.TimestampType.withoutZone()),
                optional(4, "v", Types.DoubleType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").day("ts").build();
        Table t = catalog.createTable(id("parted"), schema, spec, Map.of("format-version", "2"));
        String[] regions = {"eu", "eu", "us", "us", "asia", "asia"};
        int[] days = {1, 1, 1, 1, 2, 2};
        Map<String, List<Record>> byPartition = new LinkedHashMap<>();
        for (int n = 0; n < regions.length; n++) {
            Record r = row(schema, "id", (long) n + 1, "region", regions[n],
                    "ts", LocalDateTime.of(2024, 3, days[n], 8 + n, 0), "v", n * 0.5d);
            byPartition.computeIfAbsent(regions[n] + "/" + days[n], k -> new ArrayList<>()).add(r);
        }
        var append = t.newAppend();
        for (List<Record> rows : byPartition.values()) {
            PartitionKey key = new PartitionKey(spec, schema);
            key.partition(new InternalRecordWrapper(schema.asStruct()).wrap(rows.get(0)));
            append.appendFile(writeData(t, rows, key));
        }
        append.commit();
        return t;
    }

    private static Table deletes(Catalog catalog) throws IOException {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "name", Types.StringType.get()));
        Table t = catalog.createTable(id("deletes"), schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        List<Record> rows = new ArrayList<>();
        for (long n = 1; n <= 5; n++) {
            rows.add(row(schema, "id", n, "name", "n" + n));
        }
        DataFile f1 = writeData(t, rows, null);
        t.newAppend().appendFile(f1).commit();
        PositionDeleteWriter<Record> pw = Parquet.writeDeletes(
                t.io().newOutputFile(t.location() + "/data/pos-del-" + FILES.incrementAndGet() + ".parquet"))
                .withSpec(PartitionSpec.unpartitioned()).overwrite().buildPositionWriter();
        try (pw) {
            PositionDelete<Record> pd = PositionDelete.create();
            pw.write(pd.set(f1.location(), 1L));   // id 2
        }
        t.newRowDelta().addDeletes(pw.toDeleteFile()).commit();
        Schema idOnly = t.schema().select("id");
        EqualityDeleteWriter<Record> ew = Parquet.writeDeletes(
                t.io().newOutputFile(t.location() + "/data/eq-del-" + FILES.incrementAndGet() + ".parquet"))
                .forTable(t).withSpec(PartitionSpec.unpartitioned()).overwrite().rowSchema(idOnly)
                .equalityFieldIds(1).createWriterFunc(GenericParquetWriter::create)
                .buildEqualityWriter();
        try (ew) {
            ew.write(row(idOnly, "id", 4L));
        }
        DeleteFile eq = ew.toDeleteFile();
        t.newRowDelta().addDeletes(eq).commit();
        t.updateSchema().renameColumn("name", "label").commit();
        t.refresh();
        rows.clear();
        rows.add(row(t.schema(), "id", 6L, "label", "n6"));
        rows.add(row(t.schema(), "id", 7L, "label", "n7"));
        t.newAppend().appendFile(writeData(t, rows, null)).commit();
        return t;
    }

    private static Table nested(Catalog catalog) throws IOException {
        Schema schema = new Schema(
                required(1, "id", Types.LongType.get()),
                optional(2, "st", Types.StructType.of(
                        optional(3, "a", Types.IntegerType.get()),
                        optional(4, "b", Types.StringType.get()))));
        Table t = catalog.createTable(id("nested"), schema, PartitionSpec.unpartitioned(),
                Map.of("format-version", "2"));
        t.newAppend().appendFile(writeData(t, List.of(nestedRow(schema, 1, "x1"),
                nestedRow(schema, 2, "x2")), null)).commit();
        t.updateSchema().renameColumn("st.b", "c").commit();
        t.refresh();
        t.newAppend().appendFile(writeData(t, List.of(nestedRow(t.schema(), 3, "x3"),
                nestedRow(t.schema(), 4, "x4")), null)).commit();
        return t;
    }

    private static Record nestedRow(Schema schema, long id, String text) {
        Record r = GenericRecord.create(schema);
        r.setField("id", id);
        Types.StructType type = schema.findField("st").type().asStructType();
        Record st = GenericRecord.create(type);
        st.set(0, (int) id * 10);
        st.set(1, text);
        r.setField("st", st);
        return r;
    }

    private static Record row(Schema schema, Object... nameValues) {
        Record r = GenericRecord.create(schema);
        for (int i = 0; i < nameValues.length; i += 2) {
            r.setField((String) nameValues[i], nameValues[i + 1]);
        }
        return r;
    }

    private static DataFile writeData(Table t, List<Record> rows, StructLike partition)
            throws IOException {
        String name = "data/" + (partition == null ? "" : "p" + FILES.get() + "-")
                + "f" + FILES.incrementAndGet() + ".parquet";
        Parquet.DataWriteBuilder builder = Parquet.writeData(t.io().newOutputFile(t.location() + "/" + name))
                .schema(t.schema()).createWriterFunc(GenericParquetWriter::create).overwrite()
                .withSpec(t.spec());
        if (partition != null) {
            builder = builder.withPartition(partition);
        }
        DataWriter<Record> w = builder.build();
        try (w) {
            for (Record r : rows) {
                w.write(r);
            }
        }
        return w.toDataFile();
    }
}
