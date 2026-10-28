package pro.surpin.data.arrowflight.server;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TestPorts}.
 */
@Tag("unit")
class TestPortsTest {

    @Test
    void portsAreBelowTheEphemeralRangeDistinctAndBindable() throws Exception {
        Set<Integer> ports = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            int port = TestPorts.free();
            if (TestPorts.end() - TestPorts.FIRST >= 100) {
                assertTrue(port >= TestPorts.FIRST && port < TestPorts.end(), "port " + port);
            }
            assertTrue(ports.add(port), "handed out twice: " + port);
            try (ServerSocket socket = new ServerSocket(port)) {
                assertEquals(port, socket.getLocalPort());
            }
        }
    }

    @Test
    void theSystemRangeIsUsedWhereItCanBeRead() throws Exception {
        Path range = Path.of("/proc/sys/net/ipv4/ip_local_port_range");
        Assumptions.assumeTrue(Files.isReadable(range), "no Linux port range file");
        int start = Integer.parseInt(Files.readAllLines(range).get(0).trim().split("\\s+")[0]);
        assertEquals(start, TestPorts.end());
    }

    @Test
    void portTakenByAListenerIsSkipped() throws Exception {
        int port = TestPorts.free();
        try (ServerSocket taken = new ServerSocket(port + 1)) {
            int other = TestPorts.free();
            assertTrue(other != port + 1, "a listening port was handed out");
        } catch (java.net.BindException e) {
            // port + 1 was in use already; nothing to check
        }
    }

    @Test
    void ephemeralStartIsReadFromTheRangeFileWithTheLinuxDefaultOtherwise(@TempDir Path dir)
            throws Exception {
        Path range = Files.writeString(dir.resolve("range"), "40000\t60999\n");
        assertEquals(40000, TestPorts.ephemeralStart(range));
        assertEquals(32768, TestPorts.ephemeralStart(dir.resolve("missing")));
        assertEquals(32768, TestPorts.ephemeralStart(Files.writeString(dir.resolve("bad"), "x")));
    }
}
