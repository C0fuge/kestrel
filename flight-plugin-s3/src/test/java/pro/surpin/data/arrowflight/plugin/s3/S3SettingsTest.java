package pro.surpin.data.arrowflight.plugin.s3;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the S3 settings: validation, credentials sources and redaction. */
@Tag("unit")
class S3SettingsTest {

    /** Verifies defaults and the endpoint forms for the Java client and DuckDB. */
    @Test
    void endpointForms() {
        S3Settings plain = settings("http://minio:9000/", "AK", "SK");
        assertEquals("http://minio:9000", plain.endpoint());
        assertFalse(plain.useSsl());
        assertEquals("minio:9000", plain.duckDbEndpoint());
        assertEquals("us-east-1", plain.region());
        assertTrue(plain.pathStyle());

        S3Settings noScheme = settings("s3.local:9443", "AK", "SK");
        assertEquals("https://s3.local:9443", noScheme.endpoint());
        assertTrue(noScheme.useSsl());

        S3Settings aws = new S3Settings(null, "eu-west-3", "vhost", null, true, "AK", "SK", null,
                30, 8, true);
        assertEquals("https://s3.eu-west-3.amazonaws.com", aws.endpointUri().toString());
        assertEquals("s3.eu-west-3.amazonaws.com", aws.duckDbEndpoint());
        assertTrue(aws.useSsl());
        assertFalse(aws.pathStyle());
    }

    /** Verifies invalid values are refused. */
    @Test
    void invalidValuesAreRefused() {
        for (String endpoint : List.of("ftp://host", "http://user:pw@host", "http://host/path",
                "http://host?x=1", "http://")) {
            assertThrows(IllegalArgumentException.class, () -> settings(endpoint, "AK", "SK"), endpoint);
        }
        assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, null, "virtual",
                null, true, null, null, null, 30, 1, true));
        assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, "eu west", null,
                null, true, null, null, null, 30, 1, true));
        assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, null, null,
                null, true, null, null, null, -1, 1, true));
        assertThrows(IllegalArgumentException.class, () -> settings("http://h:1", "AK", null));
        for (int[] retry : new int[][] {{-1, 200, 2}, {21, 200, 2}, {6, -1, 2}, {6, 60_001, 2},
                {6, 200, 0}, {6, 200, 11}}) {
            assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, null, null,
                    null, true, null, null, null, 30, 1, true, retry[0], retry[1], retry[2]),
                    Arrays.toString(retry));
        }
    }

    /** Verifies DuckDB's retries default to about 6 s in all and can be configured. */
    @Test
    void httpRetriesFromTheConfiguration() {
        Map<String, String> values = new HashMap<>();
        S3Settings defaults = S3Settings.from(new MapPluginConfig(values, Map.of()), 4, false);
        assertEquals(6, defaults.httpRetries());
        assertEquals(200, defaults.httpRetryWaitMs());
        assertEquals(2, defaults.httpRetryBackoff());

        values.putAll(Map.of("httpRetries", "0", "httpRetryWaitMs", "50", "httpRetryBackoff", "1"));
        S3Settings configured = S3Settings.from(new MapPluginConfig(values, Map.of()), 4, false);
        assertEquals(0, configured.httpRetries());
        assertEquals(50, configured.httpRetryWaitMs());
        assertEquals(1, configured.httpRetryBackoff());
        assertTrue(configured.toString().contains("httpRetries=0"), configured.toString());

        values.put("httpRetries", "21");
        assertThrows(IllegalArgumentException.class,
                () -> S3Settings.from(new MapPluginConfig(values, Map.of()), 4, false));
    }

    /** Verifies toString shows neither the secret key nor the session token. */
    @Test
    void toStringRedactsSecrets() {
        S3Settings settings = new S3Settings("http://minio:9000", null, null, null, true,
                "AKIAEXAMPLE", "very-secret-key", "session-token-value", 30, 1, true);

        String text = settings.toString();

        assertFalse(text.contains("very-secret-key"), text);
        assertFalse(text.contains("session-token-value"), text);
        assertFalse(text.contains("AKIAEXAMPLE"), text);
        assertTrue(text.contains("secretKey=***"), text);
        assertFalse(new S3Settings.Credentials("AKIAEXAMPLE", "very-secret-key", null).toString()
                .contains("very-secret-key"));
        assertEquals("error *** and *** end",
                settings.redact("error very-secret-key and session-token-value end"));
    }

    /** Verifies credentials come from the environment. */
    @Test
    void credentialsFromTheEnvironment() throws IOException {
        S3Settings.Credentials credentials = S3Settings.loadCredentials(null, Map.of(
                S3Settings.ACCESS_KEY_ENV, "AK", S3Settings.SECRET_KEY_ENV, "SK",
                "AWS_SECRET_ACCESS_KEY", "ignored"));

        assertEquals("AK", credentials.accessKey());
        assertEquals("SK", credentials.secretKey());
        assertNull(credentials.sessionToken());
        assertNull(S3Settings.loadCredentials("", Map.of()).accessKey());
    }

    /** Verifies a credentials file wins over the environment and must not be world-readable. */
    @Test
    void credentialsFromAFile(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("s3.properties");
        Files.writeString(file, "accessKey=FILEAK\nsecretKey=FILESK\nsessionToken=TOKEN\n");
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r-----"));

        S3Settings.Credentials credentials = S3Settings.loadCredentials(file.toString(), Map.of(
                S3Settings.ACCESS_KEY_ENV, "AK", S3Settings.SECRET_KEY_ENV, "SK"));

        assertEquals("FILEAK", credentials.accessKey());
        assertEquals("FILESK", credentials.secretKey());
        assertEquals("TOKEN", credentials.sessionToken());

        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> S3Settings.loadCredentials(file.toString(), Map.of()));
        assertTrue(e.getMessage().contains("readable by every user"), e.getMessage());

        Path incomplete = dir.resolve("incomplete.properties");
        Files.writeString(incomplete, "accessKey=FILEAK\n");
        Files.setPosixFilePermissions(incomplete, PosixFilePermissions.fromString("rw-------"));
        assertThrows(IllegalArgumentException.class,
                () -> S3Settings.loadCredentials(incomplete.toString(), Map.of()));
    }

    /** Verifies an S3 data directory needs credentials and a readable CA file when one is set. */
    @Test
    void usableSettingsNeedCredentials(@TempDir Path dir) {
        IllegalArgumentException noKeys = assertThrows(IllegalArgumentException.class,
                () -> settings("http://h:1", null, null).requireUsable());
        assertTrue(noKeys.getMessage().contains(S3Settings.ACCESS_KEY_ENV), noKeys.getMessage());

        settings("http://h:1", "AK", "SK").requireUsable();
        assertThrows(IllegalArgumentException.class, () -> new S3Settings("http://h:1", null, null,
                dir.resolve("missing.pem").toString(), true, "AK", "SK", null, 30, 1, true).requireUsable());
    }

    /** Verifies the settings read from the s3 namespace, defaults included. */
    @Test
    void settingsFromThePluginConfiguration() {
        S3Settings defaults = S3Settings.from(new MapPluginConfig(Map.of(), Map.of(
                "accessKey", "AK", "secretKey", "SK")), 16, false);
        assertNull(defaults.endpoint());
        assertEquals("us-east-1", defaults.region());
        assertEquals("path", defaults.urlStyle());
        assertTrue(defaults.verifyTls());
        assertEquals(30, defaults.listingTtlSeconds());
        assertEquals(16, defaults.maxConnections());
        assertTrue(defaults.metadataCache());
        assertFalse(defaults.hasCredentials(), "credentials only for an S3 data directory");

        S3Settings set = S3Settings.from(new MapPluginConfig(Map.of("endpoint", "http://minio:9000",
                "region", "eu-west-3", "urlStyle", "VHOST", "listingTtlSeconds", "5",
                "maxConnections", "7", "metadataCache", "false"),
                Map.of("accessKey", "AK", "secretKey", "SK")), 16, true);
        assertEquals("http://minio:9000", set.endpoint());
        assertEquals("eu-west-3", set.region());
        assertEquals("vhost", set.urlStyle());
        assertEquals(5, set.listingTtlSeconds());
        assertEquals(7, set.maxConnections());
        assertFalse(set.metadataCache());
        assertEquals("AK", set.accessKey());
        assertEquals("SK", set.secretKey());
    }

    /** Verifies a credentials file wins over the environment. */
    @Test
    void credentialsFileWinsOverTheEnvironment(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("s3.properties");
        Files.writeString(file, "accessKey=FILEAK\nsecretKey=FILESK\n");
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        S3Settings settings = S3Settings.from(new MapPluginConfig(Map.of("credentialsFile",
                file.toString()), Map.of("accessKey", "AK", "secretKey", "SK")), 4, true);
        assertEquals("FILEAK", settings.accessKey());
        assertEquals("FILESK", settings.secretKey());
    }

    /** Verifies the AWS variables that are set are named, so the server can warn about them. */
    @Test
    void namesAwsEnvironmentVariables() {
        assertEquals(List.of("AWS_ACCESS_KEY_ID", "AWS_ENDPOINT_URL_S3"),
                S3Settings.awsEnvironmentVariables(Map.of("AWS_ACCESS_KEY_ID", "x",
                        "AWS_ENDPOINT_URL_S3", "y", "PATH", "/bin", "AWS_REGION", "")));
    }

    private static S3Settings settings(String endpoint, String accessKey, String secretKey) {
        return new S3Settings(endpoint, null, null, null, true, accessKey, secretKey, null, 30, 1, true);
    }
}
