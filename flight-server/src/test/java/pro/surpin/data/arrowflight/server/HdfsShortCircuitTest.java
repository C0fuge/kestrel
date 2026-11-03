package pro.surpin.data.arrowflight.server;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Short-circuit settings handed to the hadoopfs extension (STATUS item 26). */
@Tag("unit")
class HdfsShortCircuitTest {

    private static final String SOCKET = "/var/run/hdfs-sockets/dn";

    /** Regression: LIBHDFS3_CONF alone left the extension on its own defaults (TCP reads). */
    @Test
    void libhdfs3ConfTurnsTheExtensionSettingsOn() {
        HdfsShortCircuit.Settings s = HdfsShortCircuit.resolve(null, null,
                Map.of("dfs.client.read.shortcircuit", "true", "dfs.domain.socket.path", SOCKET),
                new Configuration(false));
        assertTrue(s.enabled());
        assertEquals("true", s.shortCircuit());
        assertEquals(SOCKET, s.socketPath());
        assertEquals("LIBHDFS3_CONF", s.source());
        assertNull(s.transferProtection());
    }

    /** A socket path in the Hadoop configuration turns short-circuit on, as for the Java client. */
    @Test
    void hadoopSocketPathTurnsShortCircuitOn() {
        Configuration hadoop = new Configuration(false);
        hadoop.set("dfs.domain.socket.path", SOCKET);
        HdfsShortCircuit.Settings s = HdfsShortCircuit.resolve(null, null, Map.of(), hadoop);
        assertTrue(s.enabled());
        assertEquals("hadoop", s.source());
    }

    /** The deployment's explicit false is kept. */
    @Test
    void hadoopFalseIsKept() {
        Configuration hadoop = new Configuration(false);
        hadoop.set("dfs.domain.socket.path", SOCKET);
        hadoop.set("dfs.client.read.shortcircuit", "false");
        assertFalse(HdfsShortCircuit.resolve(null, null, Map.of(), hadoop).enabled());
    }

    /** Explicit server settings win over both files. */
    @Test
    void serverSettingsWin() {
        HdfsShortCircuit.Settings s = HdfsShortCircuit.resolve("false", "/other", Map.of(
                "dfs.client.read.shortcircuit", "true", "dfs.domain.socket.path", SOCKET), null);
        assertFalse(s.enabled());
        assertEquals("/other", s.socketPath());
        assertEquals("server", s.source());
    }

    /** Nothing configured: the extension keeps its defaults and the configuration is unchanged. */
    @Test
    void nothingConfiguredLeavesTheConfigurationAlone() {
        HdfsShortCircuit.Settings s = HdfsShortCircuit.resolve(null, null, Map.of(),
                new Configuration(false));
        assertFalse(s.enabled());
        assertEquals("none", s.source());
        AppConfig config = ConfigAdapter.getConfig().withHdfsShortCircuit(null, null);
        assertSame(config, s.applyTo(config));
    }

    /** The resolved values reach the configuration the DuckDB adapter reads. */
    @Test
    void applyToSetsBothValues() {
        AppConfig config = ConfigAdapter.getConfig().withHdfsShortCircuit(null, null);
        AppConfig applied = HdfsShortCircuit.resolve(null, null, Map.of(
                "dfs.client.read.shortcircuit", "true", "dfs.domain.socket.path", SOCKET), null)
                .applyTo(config);
        assertEquals("true", applied.duckDbHdfsShortcircuit());
        assertEquals(SOCKET, applied.duckDbHdfsDomainSocketPath());
        assertEquals(config.dataDir(), applied.dataDir());
    }

    /** SASL data-transfer protection without short-circuit stops the start with the reason. */
    @Test
    void transferProtectionWithoutShortCircuitIsUnreadable() {
        HdfsShortCircuit.Settings s = HdfsShortCircuit.resolve(null, null,
                Map.of("dfs.data.transfer.protection", "authentication"), null);
        String reason = HdfsShortCircuit.unreadable(s).orElseThrow();
        assertTrue(reason.contains("dfs.data.transfer.protection=authentication"), reason);
        assertTrue(reason.contains("short-circuit is off"), reason);
    }

    /** SASL protection with short-circuit on but no socket on this node stops the start. */
    @Test
    void transferProtectionWithMissingSocketIsUnreadable(@TempDir Path dir) {
        Configuration hadoop = new Configuration(false);
        hadoop.set("dfs.data.transfer.protection", "privacy");
        hadoop.set("dfs.domain.socket.path", dir.resolve("missing").toString());
        String reason = HdfsShortCircuit.unreadable(
                HdfsShortCircuit.resolve(null, null, Map.of(), hadoop)).orElseThrow();
        assertTrue(reason.contains("does not exist on this node"), reason);
    }

    /** SASL protection with a present socket, or no protection at all, starts. */
    @Test
    void readableCases(@TempDir Path dir) throws Exception {
        Path socket = Files.createFile(dir.resolve("dn"));
        Map<String, String> conf = Map.of("dfs.data.transfer.protection", "authentication",
                "dfs.client.read.shortcircuit", "true", "dfs.domain.socket.path", socket.toString());
        assertTrue(HdfsShortCircuit.unreadable(
                HdfsShortCircuit.resolve(null, null, conf, null)).isEmpty());
        assertTrue(HdfsShortCircuit.unreadable(
                HdfsShortCircuit.resolve(null, null, Map.of(), null)).isEmpty());
        assertTrue(HdfsShortCircuit.socketPresent("/var/lib/hadoop-hdfs/dn._PORT"));
    }

    /** The LIBHDFS3_CONF reader the server uses returns the file's properties. */
    @Test
    void libhdfs3PropertiesReadsTheFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("hdfs-site.xml");
        Files.writeString(file, "<?xml version=\"1.0\"?><configuration><property><name>"
                + "dfs.domain.socket.path</name><value> " + SOCKET + " </value></property>"
                + "</configuration>");
        assertEquals(SOCKET, HadoopArrowFlightServer.libhdfs3Properties(file.toString())
                .get("dfs.domain.socket.path"));
    }
}
