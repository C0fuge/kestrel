package pro.surpin.data.arrowflight.server.plugin;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the namespaced plugin configuration. The s3 keys below are the contract the bench command
 * lines depend on: every flat name the server read before plugins still works, in the same order
 * within each source (system properties, then environment, then files).
 */
@Tag("unit")
class PropertiesPluginConfigTest {

    /** The s3 plugin's keys, as it declares them. */
    private static final List<ConfigKey> S3_KEYS = List.of(
            ConfigKey.of("endpoint", "", null, "s3Endpoint"),
            ConfigKey.of("region", "", "us-east-1", "s3Region"),
            ConfigKey.of("urlStyle", "", "path", "s3UrlStyle"),
            ConfigKey.of("caFile", "", null, "s3CaFile"),
            ConfigKey.of("verifyTls", "", "true", "s3VerifyTls"),
            ConfigKey.of("credentialsFile", "", null, "s3CredentialsFile"),
            ConfigKey.of("listingTtlSeconds", "", "30", "s3ListingTtlSeconds"),
            ConfigKey.of("maxConnections", "", "0", "s3MaxConnections"),
            ConfigKey.of("metadataCache", "", "true", "duckDbS3MetadataCache",
                    "arrowflight.duckdb.s3MetadataCache"),
            ConfigKey.secret("accessKey", "", "s3AccessKey"),
            ConfigKey.secret("secretKey", "", "s3SecretKey"),
            ConfigKey.secret("sessionToken", "", "s3SessionToken"));

    private final Map<String, String> system = new HashMap<>();
    private final Properties file = new Properties();
    private final Map<String, String> environment = new HashMap<>();

    @Test
    void defaultsAndLegacyNamesInTheirOldOrder() {
        PropertiesPluginConfig config = config(null);
        assertEquals(Optional.of("us-east-1"), config.get("region"));
        assertEquals(Optional.empty(), config.get("endpoint"));
        assertEquals(30, config.getInt("listingTtlSeconds", -1));
        assertTrue(config.getBoolean("metadataCache", false));

        file.setProperty("s3.endpoint", "http://ns:1");
        assertEquals("http://ns:1", config(null).get("endpoint", null));
        file.setProperty("arrowflight.s3.endpoint", "http://file-canonical:1");
        assertEquals("http://file-canonical:1", config(null).get("endpoint", null));
        file.setProperty("s3Endpoint", "http://file-legacy:1");
        assertEquals("http://file-legacy:1", config(null).get("endpoint", null));
        environment.put("FLIGHT_S3_ENDPOINT", "http://env:1");
        assertEquals("http://env:1", config(null).get("endpoint", null));
        system.put("arrowflight.s3.endpoint", "http://sys-canonical:1");
        assertEquals("http://sys-canonical:1", config(null).get("endpoint", null));
        system.put("s3Endpoint", "  http://sys-legacy:1  ");
        assertEquals("http://sys-legacy:1", config(null).get("endpoint", null));
        system.put("s3Endpoint", " ");
        assertEquals("http://sys-canonical:1", config(null).get("endpoint", null));

        system.put("arrowflight.duckdb.s3MetadataCache", "false");
        assertFalse(config(null).getBoolean("metadataCache", true));
        environment.put("FLIGHT_S3_LISTING_TTL_SECONDS", "5");
        assertEquals(5, config(null).getInt("listingTtlSeconds", 30));
        system.put("s3MaxConnections", "x");
        assertThrows(IllegalArgumentException.class, () -> config(null).getInt("maxConnections", 0));
    }

    @Test
    void theEnvironmentBeatsTheBundledFile() {
        // Regression: arrowflight.properties ships s3Region=us-east-1, which hid FLIGHT_S3_REGION.
        file.setProperty("s3Region", "us-east-1");
        file.setProperty("s3UrlStyle", "path");
        environment.put("FLIGHT_S3_REGION", "eu-west-3");
        assertEquals("eu-west-3", config(null).get("region", null));
        assertEquals("path", config(null).get("urlStyle", null));
        system.put("arrowflight.s3.region", "eu-north-1");
        assertEquals("eu-north-1", config(null).get("region", null));
    }

    @Test
    void pluginFileComesBetweenEnvironmentAndBundledFile(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("s3.properties"), "urlStyle=vhost\nregion=eu-west-3\n");
        file.setProperty("s3UrlStyle", "path");
        file.setProperty("s3Region", "us-east-1");
        system.put("s3Region", "eu-north-1");
        PropertiesPluginConfig config = config(dir);
        assertEquals("vhost", config.get("urlStyle", null));
        assertEquals("eu-north-1", config.get("region", null));
        environment.put("FLIGHT_S3_URL_STYLE", "path");
        assertEquals("path", config(dir).get("urlStyle", null));
        system.put("arrowflight.s3.urlStyle", "vhost");
        assertEquals("vhost", config(dir).get("urlStyle", null));
    }

    @Test
    void secretsOnlyFromTheEnvironmentOrAFile(@TempDir Path dir) throws Exception {
        environment.put("FLIGHT_S3_SECRET_KEY", "SK");
        assertEquals(Optional.of("SK"), config(null).secret("secretKey"));
        assertEquals(Optional.empty(), config(null).secret("sessionToken"));
        assertThrows(IllegalArgumentException.class, () -> config(null).get("secretKey"));

        Path secretFile = Files.writeString(dir.resolve("token"), "TOKEN\n");
        Files.setPosixFilePermissions(secretFile, PosixFilePermissions.fromString("rw-------"));
        system.put("arrowflight.s3.sessionTokenFile", secretFile.toString());
        assertEquals(Optional.of("TOKEN"), config(null).secret("sessionToken"));
        Files.setPosixFilePermissions(secretFile, PosixFilePermissions.fromString("rw-r--r--"));
        IllegalArgumentException open = assertThrows(IllegalArgumentException.class,
                () -> config(null).secret("sessionToken"));
        assertTrue(open.getMessage().contains("readable by every user"), open.getMessage());
        system.remove("arrowflight.s3.sessionTokenFile");

        for (String name : List.of("s3SecretKey", "arrowflight.s3.secretKey", "s3.secretKey")) {
            for (boolean asSystemProperty : new boolean[] {true, false}) {
                system.clear();
                file.clear();
                if (asSystemProperty) {
                    system.put(name, "do-not-pass-me-here");
                } else {
                    file.setProperty(name, "do-not-pass-me-here");
                }
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> config(null).requireNoSecretProperties(), name);
                assertTrue(e.getMessage().contains(name), e.getMessage());
                assertTrue(e.getMessage().contains("FLIGHT_S3_SECRET_KEY"), e.getMessage());
                assertFalse(e.getMessage().contains("do-not-pass-me-here"), e.getMessage());
                assertThrows(IllegalArgumentException.class, () -> config(null).secret("secretKey"));
            }
        }
        system.clear();
        file.clear();
        Files.writeString(dir.resolve("s3.properties"), "accessKey=AK\n");
        assertThrows(IllegalArgumentException.class, () -> config(dir).requireNoSecretProperties());
        system.put("s3AccessKey", "AK");
        assertThrows(IllegalArgumentException.class, () -> config(null).requireNoSecretProperties());
    }

    private PropertiesPluginConfig config(Path pluginDir) {
        Map<String, String> sys = Map.copyOf(system);
        return new PropertiesPluginConfig("s3",
                new ConfigSource(sys::get, file, environment, pluginDir), S3_KEYS);
    }
}
