package pro.surpin.data.arrowflight.client;

import io.netty.util.Version;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the Netty floor of the connector classpath (docs/dev/DEPENDENCY-AUDIT.md).
 */
@Tag("unit")
class NettyFloorTest {

    @Test
    void nettyModulesShareOneVersionAtTheFloor() {
        Map<String, Version> modules = Version.identify();
        assertTrue(modules.containsKey("netty-codec-http2"), "gRPC needs netty-codec-http2: " + modules.keySet());
        Set<String> versions = new TreeSet<>();
        modules.values().forEach(v -> versions.add(v.artifactVersion()));
        assertEquals(1, versions.size(), "one Netty version for every module: " + modules);
        String[] parts = versions.iterator().next().split("[.-]");
        int minor = Integer.parseInt(parts[0]) * 1000 + Integer.parseInt(parts[1]);
        assertTrue(minor > 4002 || minor == 4002 && Integer.parseInt(parts[2]) >= 17,
                "Netty " + versions + " is below 4.2.17");
    }

    @Test
    void nettyAllIsNotOnTheClasspath() {
        assertFalse(Version.identify().containsKey("netty-all"), "netty-all is back on the classpath");
    }
}
