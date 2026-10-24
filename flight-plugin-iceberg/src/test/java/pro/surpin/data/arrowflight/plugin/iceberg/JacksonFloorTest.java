package pro.surpin.data.arrowflight.plugin.iceberg;

import com.fasterxml.jackson.core.Version;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the Jackson floor the plugin bundles for Iceberg (docs/dev/DEPENDENCY-AUDIT.md).
 */
@Tag("unit")
class JacksonFloorTest {

    @Test
    void jacksonIsAtTheFloor() {
        Version core = com.fasterxml.jackson.core.json.PackageVersion.VERSION;
        Version databind = com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION;
        assertEquals(core.toString(), databind.toString(), "jackson-core and jackson-databind differ");
        assertTrue(atLeast(databind, 2, 22, 3),
                "Jackson " + databind + " is below 2.22.3");
    }

    @Test
    void versionComparison() {
        assertTrue(atLeast(new Version(2, 21, 7, null, null, null), 2, 21, 7));
        assertTrue(atLeast(new Version(2, 22, 0, null, null, null), 2, 21, 7));
        assertFalse(atLeast(new Version(2, 21, 0, null, "com.fasterxml.jackson.core", "jackson-core"), 2, 21, 7));
        assertFalse(atLeast(new Version(2, 15, 3, null, null, null), 2, 21, 7));
    }

    /**
     * Tells whether a Jackson version is at least the given one ({@link Version#compareTo} also
     * compares group and artifact ids, so it cannot be used against a bare number).
     *
     * @param version version to check
     * @param major lowest major
     * @param minor lowest minor
     * @param patch lowest patch
     * @return true when the version is not below major.minor.patch
     */
    private static boolean atLeast(Version version, int major, int minor, int patch) {
        if (version.getMajorVersion() != major) {
            return version.getMajorVersion() > major;
        }
        if (version.getMinorVersion() != minor) {
            return version.getMinorVersion() > minor;
        }
        return version.getPatchLevel() >= patch;
    }
}
