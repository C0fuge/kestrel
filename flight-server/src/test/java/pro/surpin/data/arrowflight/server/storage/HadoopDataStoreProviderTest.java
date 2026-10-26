package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests the Hadoop client read options the built-in store sets. */
@Tag("unit")
class HadoopDataStoreProviderTest {

    private static final String SHORT_CIRCUIT = "dfs.client.read.shortcircuit";

    /** Regression: without a domain socket the server used to force short-circuit on and fail to start. */
    @Test
    void shortCircuitStaysOffWithoutDomainSocket(@TempDir Path dir) throws IOException {
        Configuration hadoopConfig = new HdfsConfiguration();

        try (HadoopDataStore store = HadoopDataStoreProvider.open(dir.toString(), hadoopConfig,
                131072, "localhost")) {
            assertFalse(hadoopConfig.getBoolean(SHORT_CIRCUIT, true));
            assertFalse(hadoopConfig.getBoolean("dfs.client.read.shortcircuit.skip.checksum", true));
        }
    }

    /** Verifies short-circuit is turned on when the deployment configures a domain socket. */
    @Test
    void shortCircuitOnWithDomainSocket() {
        Configuration hadoopConfig = new HdfsConfiguration();
        hadoopConfig.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");

        HadoopDataStoreProvider.applyShortCircuit(hadoopConfig);

        assertTrue(hadoopConfig.getBoolean(SHORT_CIRCUIT, false));
    }

    /** Verifies an explicit false from the deployment is kept even with a domain socket. */
    @Test
    void explicitShortCircuitOffIsKept() {
        Configuration hadoopConfig = new HdfsConfiguration();
        hadoopConfig.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
        hadoopConfig.setBoolean(SHORT_CIRCUIT, false);

        HadoopDataStoreProvider.applyShortCircuit(hadoopConfig);

        assertFalse(hadoopConfig.getBoolean(SHORT_CIRCUIT, true));
    }

    /** Verifies an explicit true from the deployment is kept (Hadoop reports a missing socket). */
    @Test
    void explicitShortCircuitOnIsKept() {
        Configuration hadoopConfig = new HdfsConfiguration();
        hadoopConfig.setBoolean(SHORT_CIRCUIT, true);

        HadoopDataStoreProvider.applyShortCircuit(hadoopConfig);

        assertTrue(hadoopConfig.getBoolean(SHORT_CIRCUIT, false));
    }

    /** Verifies values from Hadoop's bundled default files do not count as the deployment's. */
    @Test
    void bundledDefaultsAreNotExplicit() {
        Configuration hadoopConfig = new HdfsConfiguration();

        assertFalse(HadoopDataStoreProvider.explicit(hadoopConfig, SHORT_CIRCUIT));
        assertFalse(HadoopDataStoreProvider.explicit(new Configuration(), "no.such.setting"));
        hadoopConfig.set(SHORT_CIRCUIT, "true");
        assertTrue(HadoopDataStoreProvider.explicit(hadoopConfig, SHORT_CIRCUIT));
    }
}
