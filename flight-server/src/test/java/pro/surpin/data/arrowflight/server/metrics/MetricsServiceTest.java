package pro.surpin.data.arrowflight.server.metrics;

import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests Prometheus metrics collection and HTTP exposition.
 */
@Tag("unit")
class MetricsServiceTest {

    /**
     * Verifies query metrics are exposed with bounded labels.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesQueryMetrics() throws Exception {
        try (MetricsService.QueryObservation observation = MetricsService.observeQuery(
                4096L)) {
            observation.executionPath(ExecutionPath.DUCKDB_SCAN);
            observation.markFailed();
        }
        try (MetricsService service = new MetricsService(0)) {
            service.start();
            HttpResponse<String> response = get(service.port(), "/metrics");

            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type")
                    .orElseThrow().contains("version=0.0.4"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_queries_total{path=\"duckdb-scan\"}"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_query_failures_total{path=\"duckdb-scan\"}"));
            assertTrue(response.body().contains(
                    "arrowflight_parquet_logical_input_bytes_total{path=\"duckdb-scan\"}"));
            assertTrue(response.body().contains("arrowflight_jvm_threads_live"));
        }
    }

    /**
     * Verifies the query-text cache counters are exposed per cache.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesQueryCacheMetrics() throws Exception {
        CacheMetrics.oversize(CacheMetrics.Cache.STATEMENT);
        CacheMetrics.eviction(CacheMetrics.Cache.PARSE);
        try (MetricsService service = new MetricsService(0)) {
            service.start();
            String body = get(service.port(), "/metrics").body();

            for (String cache : new String[] {"parse", "schema", "statement"}) {
                assertTrue(body.contains(
                        "arrowflight_query_cache_oversize_total{cache=\"" + cache + "\"}"), cache);
                assertTrue(body.contains(
                        "arrowflight_query_cache_evictions_total{cache=\"" + cache + "\"}"), cache);
            }
            assertTrue(CacheMetrics.oversizeCount(CacheMetrics.Cache.STATEMENT) >= 1);
            assertTrue(CacheMetrics.evictionCount(CacheMetrics.Cache.PARSE) >= 1);
        }
    }

    /**
     * Verifies the health endpoint reports readiness.
     *
     * @throws Exception if the local metrics endpoint cannot be queried
     */
    @Test
    void exposesHealthEndpoint() throws Exception {
        try (MetricsService service = new MetricsService(0)) {
            service.start();
            HttpResponse<String> response = get(service.port(), "/-/healthy");

            assertEquals(200, response.statusCode());
            assertEquals("ok\n", response.body());
        }
    }

    /**
     * Reads one endpoint from a local metrics server.
     *
     * @param port local HTTP port
     * @param path endpoint path
     * @return HTTP response body and status
     * @throws Exception if the request fails
     */
    private static HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + path)).GET().build();
        return HttpClient.newHttpClient().send(request,
                HttpResponse.BodyHandlers.ofString());
    }
}
