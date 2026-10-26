package pro.surpin.data.arrowflight.server.adapters;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.model.HandleState;

import java.net.ServerSocket;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Tests the Hazelcast member the server runs. */
@Tag("integration")
class HazelcastAdapterTest {

    /**
     * Verifies the member resolves map values with the server's class loader, whatever the
     * context class loader of the thread that creates it.
     */
    @Test
    void usesTheServerClassLoaderNotTheContextClassLoader() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        AppConfig config = new AppConfig(
                1, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, port, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        Thread thread = Thread.currentThread();
        ClassLoader context = thread.getContextClassLoader();
        try (URLClassLoader foreign = new URLClassLoader("foreign", new URL[0],
                ClassLoader.getPlatformClassLoader())) {
            thread.setContextClassLoader(foreign);
            try (HazelcastAdapter adapter = new HazelcastAdapter(config)) {
                assertSame(HazelcastAdapter.class.getClassLoader(),
                        adapter.instance().getConfig().getClassLoader());
                HandleState state = HandleState.forCompleteAnswer("SELECT 1");
                adapter.statementCache().put("h", state);
                assertEquals(state, adapter.statementCache().get("h"));
            } finally {
                thread.setContextClassLoader(context);
            }
        }
    }

    /**
     * Verifies the member does not terminate in Hazelcast's own shutdown hook: a stopped server
     * leaves the cluster gracefully in the server's hook, within a few seconds.
     */
    @Test
    void leavesGracefullyInTheServerShutdownHook() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        AppConfig config = new AppConfig(
                1, 100, 1, 131072, 0, 1, 1,
                null, false, null, null,
                null, null, null, false, 1048576, 67108864, 60000L, null, null,
                32010, port, 60, 3, 1000, 30000, true, false, 64, null, null, "complete", true, true);
        try (HazelcastAdapter adapter = new HazelcastAdapter(config)) {
            assertEquals("false", adapter.instance().getConfig()
                    .getProperty("hazelcast.shutdownhook.enabled"));
            assertEquals("5", adapter.instance().getConfig()
                    .getProperty("hazelcast.graceful.shutdown.max.wait"));
        }
    }
}
