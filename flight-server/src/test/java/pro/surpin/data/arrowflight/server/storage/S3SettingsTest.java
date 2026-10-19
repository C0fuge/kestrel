package pro.surpin.data.arrowflight.server.storage;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
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
                30, 8, null, true);
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
                null, true, null, null, null, 30, 1, null, true));
        assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, "eu west", null,
                null, true, null, null, null, 30, 1, null, true));
        assertThrows(IllegalArgumentException.class, () -> new S3Settings(null, null, null,
                null, true, null, null, null, -1, 1, null, true));
        assertThrows(IllegalArgumentException.class, () -> settings("http://h:1", "AK", null));
    }

    /** Verifies toString shows neither the secret key nor the session token. */
    @Test
    void toStringRedactsSecrets() {
        S3Settings settings = new S3Settings("http://minio:9000", null, null, null, true,
                "AKIAEXAMPLE", "very-secret-key", "session-token-value", 30, 1, null, true);

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

    /** Verifies credentials as system or file properties are a startup error. */
    @Test
    void credentialPropertiesAreRefused() {
        for (String name : List.of("s3SecretKey", "arrowflight.s3.secretKey", "s3AccessKey",
                "s3SessionToken")) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> S3Settings.requireNoCredentialProperties(key -> key.equals(name) ? "x" : null));
            assertTrue(e.getMessage().contains(name), e.getMessage());
            assertTrue(e.getMessage().contains(S3Settings.SECRET_KEY_ENV), e.getMessage());
        }
        S3Settings.requireNoCredentialProperties(key -> null);
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

    /** Verifies an S3 data directory needs credentials and the httpfs extension file. */
    @Test
    void usableSettingsNeedCredentialsAndExtension(@TempDir Path dir) throws IOException {
        IllegalArgumentException noKeys = assertThrows(IllegalArgumentException.class,
                () -> settings("http://h:1", null, null).requireUsable());
        assertTrue(noKeys.getMessage().contains(S3Settings.ACCESS_KEY_ENV), noKeys.getMessage());
        IllegalArgumentException noExtension = assertThrows(IllegalArgumentException.class,
                () -> settings("http://h:1", "AK", "SK").requireUsable());
        assertTrue(noExtension.getMessage().contains("duckDbHttpfsExtension"), noExtension.getMessage());

        Path extension = Files.writeString(dir.resolve("httpfs.duckdb_extension"), "x");
        new S3Settings("http://h:1", null, null, null, true, "AK", "SK", null, 30, 1,
                extension.toString(), true).requireUsable();
        assertThrows(IllegalArgumentException.class, () -> new S3Settings("http://h:1", null, null,
                dir.resolve("missing.pem").toString(), true, "AK", "SK", null, 30, 1,
                extension.toString(), true).requireUsable());
    }

    /** Verifies the AWS variables that are set are named, so the server can warn about them. */
    @Test
    void namesAwsEnvironmentVariables() {
        assertEquals(List.of("AWS_ACCESS_KEY_ID", "AWS_ENDPOINT_URL_S3"),
                S3Settings.awsEnvironmentVariables(Map.of("AWS_ACCESS_KEY_ID", "x",
                        "AWS_ENDPOINT_URL_S3", "y", "PATH", "/bin", "AWS_REGION", "")));
    }

    private static S3Settings settings(String endpoint, String accessKey, String secretKey) {
        return new S3Settings(endpoint, null, null, null, true, accessKey, secretKey, null, 30, 1,
                null, true);
    }
}
