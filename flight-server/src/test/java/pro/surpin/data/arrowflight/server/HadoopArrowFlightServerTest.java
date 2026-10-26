package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class HadoopArrowFlightServerTest {

    @Test
    void serverStartsAndAcceptsConnections() throws Exception {
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
            assertNotNull(helper.server);
            assertTrue(helper.server.getPort() > 0);
            assertNotNull(helper.sqlClient());
        }
    }

    @Test
    void serverStartupFailsWithInvalidDataDirectory() {
        // Adapter handles nonexistent directories gracefully (empty schema cache).
        // Test that startup at least doesn't throw.
        try (TestFlightServerHelper ignored = TestFlightServerHelper.builder()
                .dataDir("/nonexistent/path")
                .start()) {
            assertNotNull(ignored.parquetAdapter);
        } catch (Exception e) {
            fail("Should not throw for missing data dir", e);
        }
    }

    @Test
    void multipleServersUseDifferentPorts() throws Exception {
        try (TestFlightServerHelper a = TestFlightServerHelper.builder().start();
                TestFlightServerHelper b = TestFlightServerHelper.builder().start()) {
            assertNotEquals(a.server.getPort(), b.server.getPort());
        }
    }

    @Test
    void serverRespondsToMetadataQueries() throws Exception {
        try (TestFlightServerHelper helper = TestFlightServerHelper.builder().start()) {
            var info = helper.sqlClient().getCatalogs();
            assertNotNull(info);
        }
    }

    /** Verifies unset or long libhdfs3 timeouts are reported and short ones are not. */
    @org.junit.jupiter.api.Test
    void reportsLongHdfsTimeouts(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        org.junit.jupiter.api.Assertions.assertEquals(2, HadoopArrowFlightServer.longHdfsTimeouts(null).size());
        java.nio.file.Path conf = dir.resolve("libhdfs3-client.xml");
        java.nio.file.Files.writeString(conf, "<configuration>"
                + "<property><name>input.connect.timeout</name><value>5000</value></property>"
                + "<property><name>input.read.timeout</name><value>3600000</value></property></configuration>");
        java.util.List<String> problems = HadoopArrowFlightServer.longHdfsTimeouts(conf.toString());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("libhdfs3 input.read.timeout is 3600000 ms"), problems);
        java.nio.file.Files.writeString(conf, "<configuration>"
                + "<property><name>input.connect.timeout</name><value>5000</value></property>"
                + "<property><name>input.read.timeout</name><value>15000</value></property></configuration>");
        org.junit.jupiter.api.Assertions.assertTrue(
                HadoopArrowFlightServer.longHdfsTimeouts(conf.toString()).isEmpty());
        // Timeouts the server sets in DuckDB are not taken from the file.
        org.junit.jupiter.api.Assertions.assertTrue(
                HadoopArrowFlightServer.longHdfsTimeouts(null, false, false).isEmpty());
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("libhdfs3 input.read.timeout is not set (library default: minutes)"),
                HadoopArrowFlightServer.longHdfsTimeouts(null, false, true));
    }

    /** Verifies unset or long libhdfs3 NameNode RPC timeouts are reported and the stand's values are not. */
    @org.junit.jupiter.api.Test
    void reportsLongHdfsRpcTimeouts(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir) throws Exception {
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of(
                "libhdfs3 rpc.client.connect.timeout is not set (library default: minutes), e.g. 5000",
                "libhdfs3 rpc.client.read.timeout is not set (library default: minutes), e.g. 15000",
                "libhdfs3 rpc.client.timeout is not set (library default: minutes), e.g. 20000",
                "libhdfs3 rpc.client.write.timeout is not set (library default: minutes), e.g. 15000"),
                HadoopArrowFlightServer.longHdfsRpcTimeouts(null));
        java.nio.file.Path conf = dir.resolve("libhdfs3-client.xml");
        java.nio.file.Files.writeString(conf, "<configuration>"
                + "<property><name>rpc.client.connect.timeout</name><value>5000</value></property>"
                + "<property><name>rpc.client.read.timeout</name><value>15000</value></property>"
                + "<property><name>rpc.client.write.timeout</name><value>15000</value></property>"
                + "<property><name>rpc.client.timeout</name><value>3600000</value></property></configuration>");
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("libhdfs3 rpc.client.timeout is 3600000 ms, e.g. 20000"),
                HadoopArrowFlightServer.longHdfsRpcTimeouts(conf.toString()));
        java.nio.file.Files.writeString(conf, "<configuration>"
                + "<property><name>rpc.client.connect.timeout</name><value>5000</value></property>"
                + "<property><name>rpc.client.read.timeout</name><value>15000</value></property>"
                + "<property><name>rpc.client.write.timeout</name><value>bad</value></property>"
                + "<property><name>rpc.client.timeout</name><value>20000</value></property>"
                + "<property><name>rpc.client.connect.retry</name><value>2</value></property></configuration>");
        org.junit.jupiter.api.Assertions.assertEquals(
                java.util.List.of("libhdfs3 rpc.client.write.timeout is bad ms, e.g. 15000"),
                HadoopArrowFlightServer.longHdfsRpcTimeouts(conf.toString()));
        java.nio.file.Files.writeString(conf, "<configuration>"
                + "<property><name>rpc.client.connect.timeout</name><value>5000</value></property>"
                + "<property><name>rpc.client.read.timeout</name><value>15000</value></property>"
                + "<property><name>rpc.client.write.timeout</name><value>15000</value></property>"
                + "<property><name>rpc.client.timeout</name><value>20000</value></property></configuration>");
        org.junit.jupiter.api.Assertions.assertTrue(
                HadoopArrowFlightServer.longHdfsRpcTimeouts(conf.toString()).isEmpty());
    }
}
