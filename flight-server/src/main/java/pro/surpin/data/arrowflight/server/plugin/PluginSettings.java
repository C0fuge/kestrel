package pro.surpin.data.arrowflight.server.plugin;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.CodeSource;
import java.util.List;
import java.util.Locale;

/**
 * Server settings about plugins, kept out of AppConfig.
 *
 * @param pluginsDir plugins directory ({@code arrowflight.plugins.dir}, {@code FLIGHT_PLUGINS_DIR};
 *                   default {@code ../plugins} next to the server jar), or null for none
 * @param pluginConfigDir directory of {@code <ns>.properties} files
 *                        ({@code arrowflight.plugins.configDir}; default
 *                        {@code <pluginsDir>/../etc/plugins}), or null
 * @param catalog table catalogs ({@code catalog=}, default "directory"): a comma separated list
 *                of catalog provider names; the directory catalog is always on
 * @param requireChecksums whether every loaded plugin needs a matching SHA256SUMS
 *                         ({@code arrowflight.plugins.requireChecksums})
 */
public record PluginSettings(Path pluginsDir, Path pluginConfigDir, String catalog,
        boolean requireChecksums) {

    /** The catalog of schemas and tables as directories of the data directory. */
    public static final String DIRECTORY_CATALOG = "directory";

    /**
     * Lists the catalogs besides the directory catalog, in the order configured.
     *
     * @return catalog provider names, lower case, without "directory" and duplicates
     */
    public List<String> catalogs() {
        java.util.Set<String> result = new java.util.LinkedHashSet<>();
        for (String part : catalog == null ? new String[0] : catalog.split(",")) {
            String name = part.strip().toLowerCase(Locale.ROOT);
            if (!name.isEmpty() && !DIRECTORY_CATALOG.equals(name)) {
                result.add(name);
            }
        }
        return List.copyOf(result);
    }

    /**
     * Reads the settings.
     *
     * @param source configuration source
     * @return settings
     */
    public static PluginSettings from(ConfigSource source) {
        Path pluginsDir = source.lookup(List.of("arrowflight.plugins.dir", "pluginsDir"),
                List.of("FLIGHT_PLUGINS_DIR")).map(Path::of).orElseGet(PluginSettings::besideJar);
        Path configDir = source.lookup(List.of("arrowflight.plugins.configDir"),
                List.of("FLIGHT_PLUGINS_CONFIG_DIR")).map(Path::of)
                .orElse(pluginsDir == null ? null
                        : pluginsDir.toAbsolutePath().normalize().resolveSibling("etc").resolve("plugins"));
        String catalog = source.lookup(List.of("catalog", "arrowflight.catalog"),
                List.of("FLIGHT_CATALOG")).map(c -> c.toLowerCase(Locale.ROOT))
                .orElse(DIRECTORY_CATALOG);
        boolean requireChecksums = source.lookup(List.of("arrowflight.plugins.requireChecksums"),
                List.of("FLIGHT_PLUGINS_REQUIRE_CHECKSUMS")).map(Boolean::parseBoolean).orElse(false);
        return new PluginSettings(pluginsDir, configDir, catalog, requireChecksums);
    }

    /**
     * Returns {@code ../plugins} next to the server jar.
     *
     * @return directory, or null when the server does not run from a jar
     */
    private static Path besideJar() {
        CodeSource code = PluginSettings.class.getProtectionDomain().getCodeSource();
        if (code == null || code.getLocation() == null) {
            return null;
        }
        try {
            Path location = Path.of(code.getLocation().toURI());
            Path name = location.getFileName();
            Path parent = location.getParent();
            if (name == null || parent == null || !name.toString().endsWith(".jar")) {
                return null;
            }
            return parent.resolveSibling("plugins").normalize();
        } catch (URISyntaxException | IllegalArgumentException e) {
            return null;
        }
    }
}
