package pro.surpin.data.arrowflight.server.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.spi.catalog.ColumnSpec;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/** Sources of Iceberg snapshots: equality deletes keep their columns read by every query. */
class CatalogViewsIcebergSourceTest {

    private static final String METADATA = "hdfs://ns/t/metadata/v2.metadata.json";

    private static final String SCAN = "iceberg_scan('" + METADATA + "')";

    private static TableSnapshot snapshot(DeleteFile... deletes) {
        TableFile file = new TableFile("hdfs://ns/t/data/a.parquet", 10, 0, Set.of(), Map.of(), 5,
                List.of(deletes));
        List<ColumnSpec> columns = List.of(new ColumnSpec("id", "BIGINT", false, 1),
                new ColumnSpec("status", "VARCHAR", true, 2), new ColumnSpec("v", "DOUBLE", true, 3));
        return new TableSnapshot(new TableName("ice", "t"), "1.0", "f", columns, List.of(),
                List.of(file), new ReadSpec.IcebergMetadata(METADATA));
    }

    private static DeleteFile equality(Integer... fieldIds) {
        return new DeleteFile("hdfs://ns/t/data/e.parquet", 1, DeleteFile.Kind.EQUALITY,
                List.of(fieldIds));
    }

    /** Verifies that tables without equality deletes are read by the plain scan. */
    @Test
    void withoutEqualityDeletesThePlainScan() {
        assertEquals(SCAN, CatalogViews.icebergSource(METADATA, snapshot()));
        assertEquals(SCAN, CatalogViews.icebergSource(METADATA, snapshot(new DeleteFile(
                "hdfs://ns/t/data/p.parquet", 1, DeleteFile.Kind.POSITION, List.of()))));
    }

    /** Verifies that equality deletes keep exactly their columns read, below a LIMIT. */
    @Test
    void equalityDeletesKeepTheirColumns() {
        assertEquals("(SELECT * FROM " + SCAN + " AS eq_keep WHERE hash(eq_keep.\"id\", eq_keep.\"status\")"
                + " IS NOT NULL LIMIT " + Long.MAX_VALUE + ")",
                CatalogViews.icebergSource(METADATA, snapshot(equality(2), equality(1, 2))));
    }

    /**
     * Verifies the kept form is a FROM source DuckDB accepts with filters and joins, and that a
     * filter on a column outside the equality columns is applied above it.
     */
    @Test
    void keptFormIsAValidSource() throws Exception {
        String source = CatalogViews.icebergSource(METADATA, snapshot(equality(1)))
                .replace(SCAN, "(SELECT range AS id, range % 3 AS v FROM range(10))");
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM " + source + " a JOIN " + source
                    + " b USING (id) WHERE a.id > 4")) {
                rs.next();
                assertEquals(5, rs.getLong(1));
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM " + source + " WHERE v = 0")) {
                rs.next();
                assertEquals(4, rs.getLong(1));
            }
        }
    }
}
