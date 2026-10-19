package pro.surpin.data.arrowflight.server;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.CatalogViews;
import pro.surpin.data.arrowflight.server.storage.HadoopDataStore;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Golden tests (b) and (d) of the plugin plan over the test TPC-H data: the statements of the
 * complete-answer views ({@link CatalogViews.Snapshot}) and the "Parsed DDL" text
 * {@link ParquetAdapter} logs at startup, plus that DDL for every Arrow type.
 */
@Tag("unit")
class GoldenCatalogTest {

    @TempDir
    Path dir;

    /** Verifies the view statements and the logged DDL of the test data against golden files. */
    @Test
    void viewsAndParsedDdlOfTheTestData() throws Exception {
        Path data = testData(dir.resolve("data"));
        AppConfig config = config(data.toString(), true);
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        HadoopDataStore store = new HadoopDataStore(localFs, data.toString(), "localhost");

        PrintStream err = System.err;
        ByteArrayOutputStream log = new ByteArrayOutputStream();
        ParquetAdapter parquet;
        try {
            System.setErr(new PrintStream(log, true, StandardCharsets.UTF_8));
            parquet = new ParquetAdapter(config, store, "localhost");
        } finally {
            System.setErr(err);
        }
        String logged = log.toString(StandardCharsets.UTF_8);
        String marker = "Parsed DDL: ";
        int start = logged.indexOf(marker);
        assertTrue(start >= 0, logged);
        int end = logged.indexOf("\n[", start);
        String ddl = logged.substring(start + marker.length(), end < 0 ? logged.length() : end);
        Golden.assertMatches("parsed-ddl.txt", ddl.replace(data.toString(), "<DATA>"));

        StringBuilder out = new StringBuilder();
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try (DuckDbAdapter duck = new DuckDbAdapter(config, ioPool, store.duckDbRoots())) {
            for (boolean widen : new boolean[] {true, false}) {
                CatalogViews.Snapshot snapshot = new CatalogViews(parquet, duck, widen).current();
                out.append("# widenUnsignedTypes=").append(widen).append('\n');
                append(out, "data", snapshot.dataStatements());
                append(out, "empty", snapshot.emptyStatements());
                append(out, "drop", snapshot.dropStatements());
            }
        } finally {
            ioPool.shutdownNow();
        }
        Golden.assertMatches("catalog-views.txt", out.toString().replace(data.toString(), "<DATA>"));
    }

    /** Verifies the DDL type names of every Arrow type against the golden file. */
    @Test
    void ddlTypeNamesOfEveryArrowType() throws Exception {
        LocalFileSystem localFs = new LocalFileSystem();
        localFs.initialize(URI.create("file:///"), new Configuration());
        Path empty = Files.createDirectories(dir.resolve("empty"));
        ParquetAdapter parquet = new ParquetAdapter(config(empty.toString(), true),
                new HadoopDataStore(localFs, empty.toString(), "localhost"));
        List<Field> fields = new ArrayList<>();
        int i = 0;
        for (ArrowType type : everyArrowType()) {
            fields.add(new Field("c" + i++ + " " + type, FieldType.nullable(type), children(type)));
            fields.add(new Field("n" + i++ + " " + type, FieldType.notNullable(type), children(type)));
        }
        String ddl = parquet.arrowSchemaToDDL("s", "every_type", new Schema(fields)) + "\n"
                + parquet.arrowSchemaToDDL(null, "no_schema", new Schema(List.of(
                        Field.nullable("x", new ArrowType.Int(32, true))))) + "\n";
        Golden.assertMatches("ddl-type-names.txt", ddl);
    }

    /**
     * Lists every Arrow type the DDL may meet, with the parameters that change its name.
     *
     * @return Arrow types
     */
    private static List<ArrowType> everyArrowType() {
        List<ArrowType> types = new ArrayList<>();
        types.add(ArrowType.Null.INSTANCE);
        types.add(ArrowType.Bool.INSTANCE);
        for (int width : new int[] {8, 16, 32, 64}) {
            types.add(new ArrowType.Int(width, true));
            types.add(new ArrowType.Int(width, false));
        }
        types.add(new ArrowType.Int(24, true));
        for (FloatingPointPrecision precision : FloatingPointPrecision.values()) {
            types.add(new ArrowType.FloatingPoint(precision));
        }
        types.add(new ArrowType.Decimal(4, 1, 128));
        types.add(new ArrowType.Decimal(15, 2, 128));
        types.add(new ArrowType.Decimal(18, 0, 128));
        types.add(new ArrowType.Decimal(38, 10, 128));
        types.add(new ArrowType.Decimal(76, 20, 256));
        types.add(new ArrowType.Decimal(19, 19, 128));
        types.add(new ArrowType.Decimal(20, 0, 128));
        types.add(new ArrowType.Decimal(1, 0, 128));
        types.add(new ArrowType.Decimal(0, 0, 128));
        types.add(new ArrowType.Decimal(10, -2, 128));
        types.add(ArrowType.Utf8.INSTANCE);
        types.add(ArrowType.LargeUtf8.INSTANCE);
        types.add(ArrowType.Utf8View.INSTANCE);
        types.add(ArrowType.Binary.INSTANCE);
        types.add(ArrowType.LargeBinary.INSTANCE);
        types.add(ArrowType.BinaryView.INSTANCE);
        types.add(new ArrowType.FixedSizeBinary(16));
        types.add(new ArrowType.Date(DateUnit.DAY));
        types.add(new ArrowType.Date(DateUnit.MILLISECOND));
        types.add(new ArrowType.Time(TimeUnit.SECOND, 32));
        types.add(new ArrowType.Time(TimeUnit.MILLISECOND, 32));
        types.add(new ArrowType.Time(TimeUnit.MICROSECOND, 64));
        types.add(new ArrowType.Time(TimeUnit.NANOSECOND, 64));
        for (TimeUnit unit : TimeUnit.values()) {
            types.add(new ArrowType.Timestamp(unit, null));
            types.add(new ArrowType.Timestamp(unit, "UTC"));
        }
        for (IntervalUnit unit : IntervalUnit.values()) {
            types.add(new ArrowType.Interval(unit));
        }
        for (TimeUnit unit : TimeUnit.values()) {
            types.add(new ArrowType.Duration(unit));
        }
        types.add(ArrowType.List.INSTANCE);
        types.add(ArrowType.LargeList.INSTANCE);
        types.add(new ArrowType.FixedSizeList(3));
        types.add(ArrowType.Struct.INSTANCE);
        types.add(new ArrowType.Map(false));
        types.add(new ArrowType.Union(UnionMode.Sparse, new int[] {0}));
        return types;
    }

    /**
     * Builds the child fields a nested type needs to be valid.
     *
     * @param type Arrow type
     * @return child fields, empty for flat types
     */
    private static List<Field> children(ArrowType type) {
        Field item = Field.nullable("item", new ArrowType.Int(32, true));
        return switch (type.getTypeID()) {
            case List, LargeList, FixedSizeList, Union -> List.of(item);
            case Struct -> List.of(item, Field.nullable("name", ArrowType.Utf8.INSTANCE));
            case Map -> List.of(new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    List.of(Field.notNullable("key", ArrowType.Utf8.INSTANCE), item)));
            default -> List.of();
        };
    }

    /**
     * Lays out the test TPC-H data and a few tables the views treat specially.
     *
     * @param data data directory to create
     * @return the data directory
     * @throws Exception when a file cannot be written
     */
    private static Path testData(Path data) throws Exception {
        copy("/lineitem_tiny/tpch/lineitem/lineitem_tiny.parquet",
                data.resolve("tpch/lineitem/lineitem_tiny.parquet"));
        copy("/test_db/test_schema/test_table/part-00000-7b025801-1c73-42d1-804c-0a29f02e9942-c000.snappy.parquet",
                data.resolve("test_schema/test_table/part-00000.snappy.parquet"));
        // A second schema with a table of the same name: no schema-less alias for "lineitem".
        copy("/lineitem_tiny/tpch/lineitem/lineitem_tiny.parquet",
                data.resolve("other/LineItem/part-0.parquet"));
        Files.createDirectories(data.resolve("tpch/empty_table"));
        Files.writeString(data.resolve("tpch/empty_table/_SUCCESS"), "");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            Path nation = Files.createDirectories(data.resolve("tpch/nation"));
            s.execute("COPY (SELECT range::UTINYINT AS n_nationkey, 'N' || range AS n_name,"
                    + " (range % 5)::USMALLINT AS n_regionkey, range::UINTEGER AS n_u32,"
                    + " range::UBIGINT AS n_u64 FROM range(0, 13)) TO '"
                    + nation.resolve("part-0.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT range::UTINYINT AS n_nationkey, 'N' || range AS n_name,"
                    + " (range % 5)::USMALLINT AS n_regionkey, range::UINTEGER AS n_u32,"
                    + " range::UBIGINT AS n_u64 FROM range(13, 25)) TO '"
                    + nation.resolve("part-1.parquet") + "' (FORMAT parquet)");
            Path types = Files.createDirectories(data.resolve("types/all_types"));
            s.execute("COPY (SELECT true AS b, 1::TINYINT AS i8, 2::SMALLINT AS i16, 3::INTEGER AS i32,"
                    + " 4::BIGINT AS i64, 1.5::FLOAT AS f32, 2.5::DOUBLE AS f64,"
                    + " 12.3::DECIMAL(4,1) AS d4, 1234.567::DECIMAL(18,3) AS d18,"
                    + " 1::DECIMAL(38,10) AS d38, 'x' AS s, '\\x01'::BLOB AS bin,"
                    + " DATE '2024-01-02' AS dt, TIME '01:02:03' AS tm,"
                    + " TIMESTAMP '2024-01-02 03:04:05' AS ts, TIMESTAMPTZ '2024-01-02 03:04:05+00' AS tstz,"
                    + " 'Mixed Case' AS \"Mixed Case\", [1, 2] AS lst, {'a': 1} AS st"
                    + ") TO '" + types.resolve("part-0.parquet") + "' (FORMAT parquet)");
            Path reserved = Files.createDirectories(data.resolve("main/t"));
            s.execute("COPY (SELECT 1 AS x) TO '" + reserved.resolve("part-0.parquet")
                    + "' (FORMAT parquet)");
        }
        return data;
    }

    /**
     * Copies a test resource to a file.
     *
     * @param resource resource name
     * @param target file to write
     * @throws Exception when the resource cannot be copied
     */
    private static void copy(String resource, Path target) throws Exception {
        Files.createDirectories(target.getParent());
        try (InputStream in = GoldenCatalogTest.class.getResourceAsStream(resource)) {
            Files.copy(in, target);
        }
    }

    /**
     * Appends a list of statements under a heading.
     *
     * @param out text to append to
     * @param heading heading
     * @param statements statements, one per line
     */
    private static void append(StringBuilder out, String heading, List<String> statements) {
        out.append("## ").append(heading).append('\n');
        for (String statement : statements) {
            out.append(statement).append('\n');
        }
    }

    /**
     * Builds a server configuration over a data directory.
     *
     * @param dataDir data directory
     * @param widenUnsigned whether unsigned integers are widened
     * @return configuration
     */
    private static AppConfig config(String dataDir, boolean widenUnsigned) {
        return new AppConfig(
                3, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, dataDir, null,
                32010, 5701, 60, 3, 1000, 30000, true, false, 64, null, null, "complete",
                widenUnsigned, true);
    }
}
