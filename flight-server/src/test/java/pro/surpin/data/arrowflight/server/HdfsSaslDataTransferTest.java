package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** SASL data transfer of the hadoopfs extension (STATUS item 41). */
@Tag("unit")
class HdfsSaslDataTransferTest {

    /** LIBHDFS3_CONF of a Kerberos cluster with data-transfer protection. */
    private static final Map<String, String> SASL_CONF = Map.of(
            "dfs.data.transfer.protection", "privacy",
            "hadoop.security.authentication", "kerberos");

    /**
     * Builds a configuration source with system properties and environment variables.
     *
     * @param sys system properties
     * @param env environment
     * @return source
     */
    private static ConfigSource source(Map<String, String> sys, Map<String, String> env) {
        return new ConfigSource(sys::get, new Properties(), env, null);
    }

    /** Unset is auto; the property, its alias and the environment variable are read. */
    @Test
    void modeIsReadFromEveryName() {
        assertEquals(HdfsSaslDataTransfer.Mode.AUTO, HdfsSaslDataTransfer.mode(source(Map.of(), Map.of())));
        assertEquals(HdfsSaslDataTransfer.Mode.OFF,
                HdfsSaslDataTransfer.mode(source(Map.of("hdfsSaslDataTransfer", "off"), Map.of())));
        assertEquals(HdfsSaslDataTransfer.Mode.ON, HdfsSaslDataTransfer.mode(
                source(Map.of("arrowflight.hdfs.saslDataTransfer", "ON"), Map.of())));
        assertEquals(HdfsSaslDataTransfer.Mode.OFF, HdfsSaslDataTransfer.mode(
                source(Map.of(), Map.of("HDFS_SASL_DATA_TRANSFER", "false"))));
        assertThrows(IllegalArgumentException.class, () -> HdfsSaslDataTransfer.mode(
                source(Map.of("hdfsSaslDataTransfer", "maybe"), Map.of())));
    }

    /** Only our builds from afc6 on speak SASL data transfer. */
    @Test
    void supportedBuilds() {
        assertTrue(HdfsSaslDataTransfer.supportsSasl("51b7220-afc6"));
        assertTrue(HdfsSaslDataTransfer.supportsSasl("51b7220-afc12"));
        assertFalse(HdfsSaslDataTransfer.supportsSasl("51b7220-afc5"));
        assertFalse(HdfsSaslDataTransfer.supportsSasl("51b7220-afc4"));
        assertFalse(HdfsSaslDataTransfer.supportsSasl("51b7220"));
        assertFalse(HdfsSaslDataTransfer.supportsSasl("v1.5.6"));
        assertFalse(HdfsSaslDataTransfer.supportsSasl(null));
    }

    /** Auto reads directly with afc6 and keeps short-circuit only with older builds. */
    @Test
    void autoFollowsTheExtensionVersion() {
        HdfsSaslDataTransfer.Decision direct = HdfsSaslDataTransfer.decide(
                HdfsSaslDataTransfer.Mode.AUTO, Optional.of("51b7220-afc6"), SASL_CONF);
        assertTrue(direct.direct());
        assertNull(direct.reason());
        assertEquals("mode=auto direct=true extensionVersion=51b7220-afc6", direct.describe());

        HdfsSaslDataTransfer.Decision old = HdfsSaslDataTransfer.decide(
                HdfsSaslDataTransfer.Mode.AUTO, Optional.of("51b7220-afc4"), SASL_CONF);
        assertFalse(old.direct());
        assertTrue(old.reason().contains("51b7220-afc4"), old.reason());

        HdfsSaslDataTransfer.Decision unknown = HdfsSaslDataTransfer.decide(
                HdfsSaslDataTransfer.Mode.AUTO, Optional.empty(), SASL_CONF);
        assertFalse(unknown.direct());
        assertEquals("unknown", unknown.extensionVersion());
    }

    /** Off keeps short-circuit only whatever the build; on reads directly whatever the build. */
    @Test
    void onAndOffOverrideTheVersion() {
        assertFalse(HdfsSaslDataTransfer.decide(HdfsSaslDataTransfer.Mode.OFF,
                Optional.of("51b7220-afc6"), SASL_CONF).direct());
        assertTrue(HdfsSaslDataTransfer.decide(HdfsSaslDataTransfer.Mode.ON,
                Optional.of("other-build"), SASL_CONF).direct());
    }

    /**
     * The extension learns the protection only from LIBHDFS3_CONF: without it auto stays
     * short-circuit only and on stops the start, naming the missing setting.
     */
    @Test
    void missingClientSettings() {
        HdfsSaslDataTransfer.Decision noProtection = HdfsSaslDataTransfer.decide(
                HdfsSaslDataTransfer.Mode.AUTO, Optional.of("51b7220-afc6"),
                Map.of("hadoop.security.authentication", "kerberos"));
        assertFalse(noProtection.direct());
        assertTrue(noProtection.reason().contains("dfs.data.transfer.protection"), noProtection.reason());

        HdfsSaslDataTransfer.Decision simple = HdfsSaslDataTransfer.decide(
                HdfsSaslDataTransfer.Mode.AUTO, Optional.of("51b7220-afc6"),
                Map.of("dfs.data.transfer.protection", "authentication"));
        assertFalse(simple.direct());
        assertTrue(simple.reason().contains("hadoop.security.authentication=kerberos"), simple.reason());

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> HdfsSaslDataTransfer.decide(HdfsSaslDataTransfer.Mode.ON,
                        Optional.of("51b7220-afc6"), Map.of()));
        assertTrue(e.getMessage().contains("dfs.data.transfer.protection"), e.getMessage());
    }

    /** The version comes from the metadata footer of the extension file, as DuckDB writes it. */
    @Test
    void versionFromTheFooter(@TempDir Path dir) throws Exception {
        byte[] body = new byte[2048];
        byte[] footer = new byte[512];
        put(footer, 96, "CPP");
        put(footer, 128, "51b7220-afc6");
        put(footer, 160, "v1.5.6");
        put(footer, 192, "linux_amd64");
        put(footer, 224, "4");
        Path ext = dir.resolve("hadoopfs.duckdb_extension");
        Files.write(ext, body);
        Files.write(ext, footer, java.nio.file.StandardOpenOption.APPEND);
        assertEquals(Optional.of("51b7220-afc6"), HdfsSaslDataTransfer.extensionVersion(ext));

        Path tiny = dir.resolve("tiny");
        Files.write(tiny, new byte[10]);
        assertEquals(Optional.empty(), HdfsSaslDataTransfer.extensionVersion(tiny));
        assertEquals(Optional.empty(), HdfsSaslDataTransfer.extensionVersion(dir.resolve("missing")));
        assertEquals(Optional.empty(), HdfsSaslDataTransfer.extensionVersion(null));
        Path zeros = dir.resolve("zeros");
        Files.write(zeros, new byte[1024]);
        assertEquals(Optional.empty(), HdfsSaslDataTransfer.extensionVersion(zeros));
    }

    /**
     * Writes an ASCII string into a footer field.
     *
     * @param footer footer bytes
     * @param offset field offset
     * @param value value
     */
    private static void put(byte[] footer, int offset, String value) {
        byte[] b = value.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(b, 0, footer, offset, b.length);
    }
}
