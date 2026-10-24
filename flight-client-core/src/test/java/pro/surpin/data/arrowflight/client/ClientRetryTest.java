package pro.surpin.data.arrowflight.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClientRetryTest {

    @Test
    void toStringRedactsPassword() {
        Configuration config = new Configuration("localhost", 32010, "user", "my-secret-pass", null);
        String str = config.toString();

        assertFalse(str.contains("my-secret-pass"), "toString must redact password");
        assertTrue(str.contains("password='[hidden]'"), "toString must show redacted password");
    }

    @Test
    void toStringRedactsBearerToken() {
        Configuration config = new Configuration("localhost", 32010, "user", null, "my-access-token");
        String str = config.toString();

        assertFalse(str.contains("my-access-token"), "toString must redact bearer token");
        assertTrue(str.contains("bearerToken='[hidden]'"), "toString must show redacted bearer token");
    }

    @Test
    void toStringRedactsTruststorePassword() {
        Configuration config = new Configuration("localhost", 32010, "/path/to/trust.jks", "trust-pass", "user", "pass", null);
        String str = config.toString();

        assertFalse(str.contains("trust-pass"), "toString must redact truststore password");
        assertTrue(str.contains("trustStorePass='[hidden]'"), "toString must show redacted truststore pass");
    }

    @Test
    void toStringDoesNotLeakCertBytes() {
        Configuration config = new Configuration("localhost", 32010, "/path/to/trust.jks", "trust-pass", "user", "pass", null);
        String str = config.toString();

        assertTrue(str.contains("certBytes='[hidden"), "toString must redact cert bytes");
        assertFalse(str.contains("certBytes=[") && !str.contains("[hidden"), "toString must not leak raw bytes");
    }

    @Test
    void toStringShowsNullFieldsAsNotSet() {
        Configuration config = new Configuration("localhost", 32010, null, null, null);
        String str = config.toString();

        assertTrue(str.contains("password='[not set]'"));
        assertTrue(str.contains("bearerToken='[not set]'"));
    }

    // ── connection string hashing ───────────────────────────────────────────

    @Test
    void connectionStringDoesNotExposePassword() {
        Configuration config = new Configuration("localhost", 32010, "user", "my-password", null);
        String cs = config.getConnectionString();

        assertFalse(cs.contains("my-password"),
                "connection string must not contain raw password: " + cs);
    }

    @Test
    void connectionStringDoesNotExposeBearerToken() {
        Configuration config = new Configuration("localhost", 32010, "user", null, "my-token");
        String cs = config.getConnectionString();

        assertFalse(cs.contains("my-token"),
                "connection string must not contain raw bearer token: " + cs);
    }

    @Test
    void connectionStringIsDeterministic() {
        Configuration c1 = new Configuration("localhost", 32010, "user", "pass", null);
        Configuration c2 = new Configuration("localhost", 32010, "user", "pass", null);

        assertEquals(c1.getConnectionString(), c2.getConnectionString(),
                "Same credentials must produce same connection identity");
    }

    @Test
    void connectionStringDiffersForDifferentSecrets() {
        Configuration c1 = new Configuration("localhost", 32010, "user", "pass1", null);
        Configuration c2 = new Configuration("localhost", 32010, "user", "pass2", null);

        assertNotEquals(c1.getConnectionString(), c2.getConnectionString(),
                "Different secrets must produce different connection identities");
    }

    /** Checks configurations with different transport, header, retry or memory settings never share a pooled client. */
    @Test
    void connectionIdentityIncludesClientSettings() {
        Configuration base = new Configuration("localhost", 32010, "user", "pass", null);
        java.util.List<java.util.function.Consumer<Configuration>> changes = java.util.List.of(
                config -> config.setRoutingTag("tag"), config -> config.setRoutingQueue("queue"),
                config -> config.setDefaultSchema("schema"), config -> config.setRetryBackoffMs(5),
                config -> config.setMaxRetries(7), config -> config.setConnectTimeoutMs(12),
                config -> config.setAllocationLimit(1024));
        for (java.util.function.Consumer<Configuration> change : changes) {
            Configuration other = new Configuration("localhost", 32010, "user", "pass", null);
            change.accept(other);
            assertNotEquals(base.getConnectionString(), other.getConnectionString());
        }
        Configuration verified = new Configuration("localhost", 32010, true, true, "user", "pass", null);
        Configuration unverified = new Configuration("localhost", 32010, true, false, "user", "pass", null);
        assertNotEquals(verified.getConnectionString(), unverified.getConnectionString(),
                "A client built without certificate verification must not serve a verifying configuration");
    }

    /** Checks the length-prefixed settings encoding keeps shifted values apart. */
    @Test
    void connectionIdentityDoesNotConfuseShiftedSettings() {
        Configuration a = new Configuration("localhost", 32010, "user", "pass", null);
        a.setRoutingTag("ab");
        a.setRoutingQueue("c");
        Configuration b = new Configuration("localhost", 32010, "user", "pass", null);
        b.setRoutingTag("a");
        b.setRoutingQueue("bc");
        assertNotEquals(a.getConnectionString(), b.getConnectionString());
        assertFalse(a.getConnectionString().contains("ab"), "settings are hashed, not shown");
    }

    /** Verifies long Flight streams have no default whole-call deadline. */
    @Test
    void defaultConfigurationDoesNotLimitFlightStreamDuration() {
        Configuration config = new Configuration("localhost", 32010, "user", "pass", null);
        String str = config.toString();

        assertTrue(str.contains("maxRetries=3"));
        assertTrue(str.contains("retryBackoffMs=1000"));
        assertEquals(0, config.getConnectTimeoutMs());
        assertTrue(str.contains("connectTimeoutMs=0"));
    }
}
