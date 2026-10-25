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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the security floors of the server's network, JSON and Parquet libraries (docs/dev/DEPENDENCY-AUDIT.md).
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
    void jacksonModulesShareOneVersionAtTheFloor() {
        List<com.fasterxml.jackson.core.Version> modules = List.of(
                com.fasterxml.jackson.core.json.PackageVersion.VERSION,
                com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION,
                com.fasterxml.jackson.dataformat.yaml.PackageVersion.VERSION,
                com.fasterxml.jackson.datatype.jsr310.PackageVersion.VERSION,
                com.fasterxml.jackson.module.scala.DefaultScalaModule$.MODULE$.version());
        Set<String> versions = new TreeSet<>();
        modules.forEach(v -> versions.add(v.toString()));
        assertEquals(1, versions.size(), "one Jackson version for every module: " + versions);
        String version = versions.iterator().next();
        assertTrue(atLeast(version, 2, 21, 7), "Jackson " + version + " is below 2.21.7");
    }

    @Test
    void hazelcastIsAtTheFloor() {
        String version = BuildInfoProvider.getBuildInfo().getVersion();
        assertTrue(atLeast(version, 5, 7, 0), "Hazelcast " + version + " is below 5.7.0");
    }

    @Test
    void parquetCodecsAreAtTheFloor() throws ClassNotFoundException {
        String parquet = org.apache.parquet.Version.VERSION_NUMBER;
        assertTrue(atLeast(parquet, 1, 15, 0), "Parquet " + parquet + " is below 1.15.0");
        String aircompressor = implementationVersion("io.airlift.compress.zstd.ZstdDecompressor");
        assertTrue(atLeast(aircompressor, 2, 0, 3), "aircompressor " + aircompressor + " is below 2.0.3");
        String snappy = implementationVersion("org.xerial.snappy.Snappy");
        assertTrue(atLeast(snappy, 1, 1, 10, 4), "snappy-java " + snappy + " is below 1.1.10.4");
    }

    @Test
    void versionComparison() {
        assertTrue(atLeast("4.2.17.Final", 4, 2, 17));
        assertTrue(atLeast("4.3.0", 4, 2, 17));
        assertTrue(atLeast("5.7.0-SNAPSHOT", 5, 7, 0));
        assertFalse(atLeast("4.2.9.Final", 4, 2, 17));
        assertFalse(atLeast("4.1.137.Final", 4, 2, 17));
        assertFalse(atLeast("5.6.0", 5, 7, 0));
        assertTrue(atLeast("1.1.10.8", 1, 1, 10, 4));
        assertFalse(atLeast("1.1.8.3", 1, 1, 10, 4));
        assertFalse(atLeast("1.1.10", 1, 1, 10, 4));
    }

    /**
     * Reads the version of the jar holding a class from its manifest.
     *
     * @param className a class of the jar
     * @return the Implementation-Version of the jar
     * @throws ClassNotFoundException when the jar is not on the classpath
     */
    private static String implementationVersion(String className) throws ClassNotFoundException {
        String version = Class.forName(className, false, DependencyFloorTest.class.getClassLoader())
                .getPackage().getImplementationVersion();
        assertNotNull(version, className + " has no Implementation-Version");
        return version;
    }

    /**
     * Tells whether a dotted version is at least the given one.
     *
     * @param version version text such as 4.2.19.Final
     * @param want lowest version numbers, most significant first (major, minor, patch, ...)
     * @return true when the version is not below the given numbers
     */
    private static boolean atLeast(String version, int... want) {
        String[] parts = version.split("[.-]");
        for (int i = 0; i < want.length; i++) {
            int have = i < parts.length && parts[i].matches("\\d+") ? Integer.parseInt(parts[i]) : 0;
            if (have != want[i]) {
                return have > want[i];
            }
        }
        return true;
    }
}
