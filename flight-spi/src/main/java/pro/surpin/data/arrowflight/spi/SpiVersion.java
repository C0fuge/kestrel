package pro.surpin.data.arrowflight.spi;

/**
 * Version of this SPI. A plugin names the version it was built against in its descriptor
 * ({@code spi=1.0} in {@code META-INF/arrowflight/plugin.properties}); the server loads it only
 * when the major versions are equal and the plugin's minor version is not newer than the
 * server's.
 */
public final class SpiVersion {

    /** Incompatible changes. */
    public static final int MAJOR = 1;
    /** Compatible additions. */
    public static final int MINOR = 5;

    /** Not instantiable. */
    private SpiVersion() {
    }

    /**
     * Returns this version as written in a descriptor.
     *
     * @return {@code MAJOR.MINOR}
     */
    public static String current() {
        return MAJOR + "." + MINOR;
    }

    /**
     * Tells whether a plugin built against a version runs on this server.
     *
     * @param version version from a plugin descriptor, {@code major.minor}
     * @return true when the majors are equal and the plugin's minor is not newer
     * @throws IllegalArgumentException when the version is not {@code major.minor}
     */
    public static boolean supports(String version) {
        int[] parsed = parse(version);
        return parsed[0] == MAJOR && parsed[1] <= MINOR;
    }

    /**
     * Parses a version.
     *
     * @param version {@code major.minor}
     * @return major and minor
     * @throws IllegalArgumentException when the version is not {@code major.minor}
     */
    public static int[] parse(String version) {
        if (version == null || !version.trim().matches("\\d{1,4}\\.\\d{1,4}")) {
            throw new IllegalArgumentException("SPI version must be major.minor: " + version);
        }
        String[] parts = version.trim().split("\\.");
        return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
    }
}
