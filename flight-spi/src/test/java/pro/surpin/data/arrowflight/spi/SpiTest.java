package pro.surpin.data.arrowflight.spi;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;
import pro.surpin.data.arrowflight.spi.metrics.MetricSample;
import pro.surpin.data.arrowflight.spi.metrics.PluginMetrics;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the value types and helpers of the SPI. */
class SpiTest {

    @Test
    void versionCompatibility() {
        assertEquals("1.4", SpiVersion.current());
        assertTrue(SpiVersion.supports("1.0"));
        assertTrue(SpiVersion.supports("1.1"));
        assertTrue(SpiVersion.supports("1.2"));
        assertTrue(SpiVersion.supports("1.3"));
        assertTrue(SpiVersion.supports("1.4"));
        assertFalse(SpiVersion.supports("1.5"));
        assertFalse(SpiVersion.supports("2.0"));
        assertFalse(SpiVersion.supports("0.9"));
        assertThrows(IllegalArgumentException.class, () -> SpiVersion.supports("1"));
        assertThrows(IllegalArgumentException.class, () -> SpiVersion.supports(null));
        assertThrows(IllegalArgumentException.class, () -> SpiVersion.supports("1.0.0"));
    }

    @Test
    void configKeyEnvironmentNames() {
        assertEquals("FLIGHT_S3_ENDPOINT", ConfigKey.of("endpoint", "", null).environmentName("s3"));
        assertEquals("FLIGHT_S3_LISTING_TTL_SECONDS",
                ConfigKey.of("listingTtlSeconds", "", "30").environmentName("s3"));
        assertEquals("FLIGHT_S3_CA_FILE", ConfigKey.environmentName("s3", "caFile"));
        assertEquals("FLIGHT_ICEBERG_REST_URI", ConfigKey.environmentName("iceberg-rest", "uri"));
        assertEquals("FLIGHT_HMS_KERBEROS_PRINCIPAL",
                ConfigKey.environmentName("hms", "kerberos.principal"));
        assertThrows(IllegalArgumentException.class, () -> ConfigKey.of("Bad", "", null));
        assertThrows(IllegalArgumentException.class, () -> ConfigKey.of("s3.", "", null));
        ConfigKey secret = ConfigKey.secret("secretKey", "secret", "s3SecretKey");
        assertTrue(secret.secret());
        assertEquals(List.of("s3SecretKey"), secret.legacyAliases());
    }

    @Test
    void redactorReplacesLongestFirst() {
        SecretRedactor redactor = new SecretRedactor();
        redactor.add(null);
        redactor.add("");
        assertEquals("plain", redactor.redact("plain"));
        redactor.add("abc");
        redactor.add("xxabcxx");
        redactor.add("abc");
        assertEquals(2, redactor.size());
        assertEquals("*** and ***", redactor.redact("xxabcxx and abc"));
        assertNull(redactor.redact(null));
        assertFalse(redactor.toString().contains("abc"));
    }

    @Test
    void recordsCopyTheirCollections() {
        Set<String> hosts = new LinkedHashSet<>(List.of("b", "a"));
        DataFile file = new DataFile("s/t/f.parquet", "hdfs://ns/s/t/f.parquet", 1, 2, hosts);
        hosts.clear();
        assertEquals(List.of("b", "a"), List.copyOf(file.hosts()));
        assertEquals(Set.of(), new DataFile("a", "b", 0, 0, null).hosts());
        assertThrows(NullPointerException.class, () -> new DataFile(null, "b", 0, 0, null));

        TableFile tableFile = new TableFile("s3://b/t/f.parquet", 10, 0, null, Map.of("dt", "2024"),
                -1, List.of(new DeleteFile("s3://b/t/d.parquet", 5, DeleteFile.Kind.POSITION, null)));
        TableSnapshot snapshot = new TableSnapshot(new TableName("s", "t"), "1", "f", null, null,
                List.of(tableFile), new ReadSpec.ParquetFiles(false));
        assertEquals("s.t", snapshot.name().toString());
        assertEquals(List.of(), snapshot.columns());
        assertEquals(List.of(), tableFile.deletes().get(0).equalityFieldIds());
        assertThrows(NullPointerException.class, () -> new ReadSpec.IcebergMetadata(null));
    }

    @Test
    void sensitiveSecretValuesAreNotPrinted() {
        assertFalse(SecretValue.sensitive("hunter2").toString().contains("hunter2"));
        assertEquals("true", SecretValue.of(true).value());
        assertFalse(SecretValue.plain("x").sensitive());
    }

    @Test
    void metricSamplesCheckTheirNames() {
        MetricSample counter = MetricSample.counter("arrowflight_s3_cache_hits_total", "hits", 3);
        assertEquals(MetricSample.Kind.COUNTER, counter.kind());
        assertEquals(3.0, counter.value());
        assertEquals(MetricSample.Kind.GAUGE, MetricSample.gauge("arrowflight_x_bytes", "x", 1).kind());
        assertThrows(IllegalArgumentException.class, () -> MetricSample.gauge("jvm_heap", "x", 1));
        assertThrows(IllegalArgumentException.class, () -> MetricSample.gauge("arrowflight_X", "x", 1));
        assertThrows(IllegalArgumentException.class, () -> MetricSample.gauge("arrowflight_a{b=\"c\"}", "x", 1));
        assertThrows(IllegalArgumentException.class, () -> MetricSample.counter("arrowflight_hits", "x", 1));
        assertThrows(IllegalArgumentException.class, () -> MetricSample.gauge("arrowflight_a", "x\ny", 1));
        PluginMetrics.none().register(() -> List.of(counter)).close();
    }

    @Test
    void arrowScansCheckTheirPredicates() throws Exception {
        ArrowScan.Predicate in = new ArrowScan.Predicate("k", ArrowScan.Op.IN,
                java.util.Arrays.asList(1L, null));
        assertEquals(2, in.values().size());
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowScan.Predicate("k", ArrowScan.Op.EQ, List.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowScan.Predicate("k", ArrowScan.Op.IS_NULL, List.of(1L)));
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowScan.Predicate("k", ArrowScan.Op.IN, null));
        assertThrows(IllegalArgumentException.class,
                () -> new ArrowScan(List.of("a", "a"), null));
        assertThrows(IllegalArgumentException.class, () -> new ArrowScan(null, null));
        assertThrows(IllegalArgumentException.class, () -> new ArrowScan(List.of(), List.of()));
        assertThrows(NullPointerException.class, () -> new ReadSpec.ArrowStream(null));
        Catalog catalog = new Catalog() {
            @Override
            public List<String> schemas() {
                return List.of();
            }

            @Override
            public List<String> tables(String schema) {
                return List.of();
            }

            @Override
            public java.util.Optional<TableSnapshot> current(TableName table) {
                return java.util.Optional.empty();
            }

            @Override
            public List<String> readableRoots() {
                return List.of();
            }
        };
        TableSnapshot snapshot = new TableSnapshot(new TableName("s", "t"), "1", "f", null, null,
                null, new ReadSpec.ArrowStream("orc"));
        assertThrows(UnsupportedOperationException.class, () -> catalog.exportArrowScan(snapshot,
                List.of(), new ArrowScan(List.of("a"), null), 0L, null));
    }
}
