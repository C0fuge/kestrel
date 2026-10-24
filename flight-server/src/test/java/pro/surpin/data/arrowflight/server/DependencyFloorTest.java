package pro.surpin.data.arrowflight.server;

import com.hazelcast.instance.BuildInfoProvider;
import io.netty.util.Version;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the security floors of the server's network libraries (docs/dev/DEPENDENCY-AUDIT.md).
 */
@Tag("unit")
class DependencyFloorTest {

    /** Netty modules carrying advisories the gRPC server does not need (they came with netty-all). */
    private static final List<String> UNUSED_NETTY = List.of(
            "netty-all", "netty-codec-redis", "netty-codec-haproxy", "netty-codec-smtp",
            "netty-codec-stomp", "netty-codec-mqtt", "netty-codec-xml", "netty-codec-http3",
            "netty-handler-ssl-ocsp", "netty-resolver-dns", "netty-transport-sctp");

    @Test
    void nettyModulesShareOneVersionAtTheFloor() {
        Map<String, Version> modules = Version.identify();
        assertTrue(modules.containsKey("netty-codec-http2"), "gRPC needs netty-codec-http2: " + modules.keySet());
        Set<String> versions = new TreeSet<>();
        modules.values().forEach(v -> versions.add(v.artifactVersion()));
        assertEquals(1, versions.size(), "one Netty version for every module: " + modules);
        String version = versions.iterator().next();
        assertTrue(atLeast(version, 4, 2, 17), "Netty " + version + " is below 4.2.17");
    }

    @Test
    void unusedNettyCodecsAreNotOnTheClasspath() {
        Set<String> modules = Version.identify().keySet();
        for (String unused : UNUSED_NETTY) {
            assertFalse(modules.contains(unused), unused + " is back on the classpath");
        }
    }

    @Test
    void hazelcastIsAtTheFloor() {
        String version = BuildInfoProvider.getBuildInfo().getVersion();
        assertTrue(atLeast(version, 5, 7, 0), "Hazelcast " + version + " is below 5.7.0");
    }

    @Test
    void versionComparison() {
        assertTrue(atLeast("4.2.17.Final", 4, 2, 17));
        assertTrue(atLeast("4.3.0", 4, 2, 17));
        assertTrue(atLeast("5.7.0-SNAPSHOT", 5, 7, 0));
        assertFalse(atLeast("4.2.9.Final", 4, 2, 17));
        assertFalse(atLeast("4.1.137.Final", 4, 2, 17));
        assertFalse(atLeast("5.6.0", 5, 7, 0));
    }

    /**
     * Tells whether a dotted version is at least the given one.
     *
     * @param version version text such as 4.2.19.Final
     * @param major lowest major
     * @param minor lowest minor
     * @param patch lowest patch
     * @return true when the version is not below major.minor.patch
     */
    private static boolean atLeast(String version, int major, int minor, int patch) {
        String[] parts = version.split("[.-]");
        int[] want = {major, minor, patch};
        for (int i = 0; i < want.length; i++) {
            int have = Integer.parseInt(parts[i]);
            if (have != want[i]) {
                return have > want[i];
            }
        }
        return true;
    }
}
