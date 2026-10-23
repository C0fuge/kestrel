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
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/** Sources of Iceberg snapshots: equality deletes are read through a materialized full scan. */
class CatalogViewsIcebergSourceTest {

    private static TableSnapshot snapshot(DeleteFile... deletes) {
        TableFile file = new TableFile("hdfs://ns/t/data/a.parquet", 10, 0, Set.of(), Map.of(), 5,
                List.of(deletes));
        return new TableSnapshot(new TableName("ice", "t"), "1.0", "f", List.of(), List.of(),
                List.of(file), new ReadSpec.IcebergMetadata("hdfs://ns/t/metadata/v2.metadata.json"));
    }

    /** Verifies that only equality deletes switch to the materialized scan. */
    @Test
    void equalityDeletesMaterializeTheScan() {
        String scan = "iceberg_scan('hdfs://ns/t/metadata/v2.metadata.json')";
        assertEquals(scan, CatalogViews.icebergSource(snapshot(), "hdfs://ns/t/metadata/v2.metadata.json"));
        assertEquals(scan, CatalogViews.icebergSource(snapshot(new DeleteFile("hdfs://ns/t/data/p.parquet",
                1, DeleteFile.Kind.POSITION, List.of())), "hdfs://ns/t/metadata/v2.metadata.json"));
        assertEquals("(WITH t AS MATERIALIZED (SELECT * FROM " + scan + ") SELECT * FROM t)",
                CatalogViews.icebergSource(snapshot(new DeleteFile("hdfs://ns/t/data/e.parquet", 1,
                        DeleteFile.Kind.EQUALITY, List.of(1))), "hdfs://ns/t/metadata/v2.metadata.json"));
    }

    /** Verifies the materialized form is a FROM source DuckDB accepts with filters and joins. */
    @Test
    void materializedFormIsAValidSource() throws Exception {
        String source = "(WITH t AS MATERIALIZED (SELECT * FROM range(10) r(id)) SELECT * FROM t)";
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT count(*) FROM " + source + " a JOIN " + source
                        + " b USING (id) WHERE a.id > 4")) {
            rs.next();
            assertEquals(5, rs.getLong(1));
        }
    }
}
