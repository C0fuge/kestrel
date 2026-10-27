package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Further S3 stores ({@code s3.stores}): names, roots, which store holds a location, and the
 * settings each store reads below {@code s3.<name>.}.
 */
@Tag("unit")
class S3NamedStoresTest {

    private final Map<String, String> values = new HashMap<>();
    private final Map<String, String> secrets = new HashMap<>();
    private final MapPluginConfig config = new MapPluginConfig(values, secrets);

    @Test
    void noStoresByDefault() {
        assertTrue(S3NamedStores.from(config).isEmpty());
        values.put("stores", " , ");
        assertTrue(S3NamedStores.from(config).isEmpty());
    }

    /** Verifies the longest root of the same bucket wins and other buckets match nothing. */
    @Test
    void theLongestRootHoldsALocation() {
        values.put("stores", "wh, deep");
        values.put("wh.root", "s3a://warehouse");
        values.put("deep.root", "s3://warehouse/team/a/");
        List<S3NamedStores.Store> stores = S3NamedStores.from(config);
        assertEquals(List.of("wh", "deep"), stores.stream().map(S3NamedStores.Store::name).toList());
        assertEquals("s3://warehouse/", stores.get(0).root().root());

        assertEquals("deep", holder(stores, "s3://warehouse/team/a/db/t"));
        assertEquals("wh", holder(stores, "s3://warehouse/team/b"));
        assertEquals("wh", holder(stores, "s3n://warehouse/team/ab"));
        assertNull(holder(stores, "s3://warehouses/x"));
        assertNull(holder(stores, "s3://lake/team/a"));
    }

    @Test
    void badStoreListsAreRefused() {
        values.put("stores", "Wh");
        assertMessage("invalid store name Wh");
        values.put("stores", "wh_2");
        assertMessage("invalid store name wh_2");
        values.put("stores", "wh,wh");
        assertMessage("names wh twice");
        values.put("stores", "wh");
        assertMessage("needs s3.wh.root");
        values.put("wh.root", "http://warehouse/x");
        assertMessage("Not an S3 URI");
        values.put("stores", "wh,other");
        values.put("wh.root", "s3://warehouse/x");
        values.put("other.root", "s3a://warehouse/x/");
        assertMessage("the same root s3://warehouse/x");
    }

    /**
     * Verifies a named store has its own endpoint, region, URL style and credentials (none of
     * them fall back to the main store's) and takes the per-connection DuckDB settings and the
     * listing settings from the main store unless it sets them.
     */
    @Test
    void namedSettingsAreTheStoresOwn() {
        values.putAll(Map.of("endpoint", "http://main:9000", "region", "eu-west-1",
                "urlStyle", "vhost", "verifyTls", "false", "listingTtlSeconds", "5",
                "metadataCache", "false", "httpRetries", "9", "connectionCaching", "false"));
        secrets.putAll(Map.of("accessKey", "MAINKEY", "secretKey", "MAINSECRET"));
        values.put("stores", "wh");
        values.put("wh.root", "s3://warehouse/");
        values.put("wh.endpoint", "https://other:9443");
        values.put("wh.listingTtlSeconds", "60");
        secrets.putAll(Map.of("wh.accessKey", "WHKEY", "wh.secretKey", "WHSECRET"));

        S3NamedStores.Store store = S3NamedStores.from(config).get(0);
        assertEquals("s3.wh", store.config().namespace());
        S3Settings settings = S3Settings.from(store.config(), 8, true);
        assertEquals("https://other:9443", settings.endpoint());
        assertEquals(S3Settings.DEFAULT_REGION, settings.region());
        assertEquals("path", settings.urlStyle());
        assertEquals("WHKEY", settings.accessKey());
        assertEquals("WHSECRET", settings.secretKey());
        assertFalse(settings.verifyTls());
        assertFalse(settings.metadataCache());
        assertEquals(60, settings.listingTtlSeconds());
        assertEquals(9, settings.httpRetries(), "inherited: one DuckDB setting per server");
        assertFalse(settings.connectionCaching(), "inherited: one DuckDB setting per server");

        secrets.remove("wh.accessKey");
        secrets.remove("wh.secretKey");
        assertFalse(S3Settings.from(store.config(), 8, true).hasCredentials(),
                "a named store never borrows the main credentials");
    }

    @Test
    void secretNames() {
        assertEquals("flight_s3", S3DataStore.secretName(""));
        assertEquals("flight_s3_wh", S3DataStore.secretName("wh"));
    }

    private static String holder(List<S3NamedStores.Store> stores, String location) {
        return S3NamedStores.holding(stores, S3Uri.parse(location))
                .map(S3NamedStores.Store::name).orElse(null);
    }

    private void assertMessage(String expected) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> S3NamedStores.from(config));
        assertTrue(e.getMessage().contains(expected), e.getMessage());
    }
}
