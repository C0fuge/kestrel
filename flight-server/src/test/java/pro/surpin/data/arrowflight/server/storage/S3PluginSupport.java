package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;

/**
 * Opens S3 data directories through the real s3 plugin, as the server does: a plugins directory
 * with a copy of flight-plugin-s3/target/plugin/s3, configuration from maps.
 */
final class S3PluginSupport {

    /** Where the s3 plugin module leaves its plugin directory. */
    static final Path PLUGIN = Path.of("../flight-plugin-s3/target/plugin/s3");

    final Path plugins;
    final Map<String, String> properties = new HashMap<>();
    final Map<String, String> environment = new HashMap<>();

    /**
     * Copies the plugin into a plugins directory.
     *
     * @param dir test directory
     * @throws IOException when the plugin cannot be copied
     */
    S3PluginSupport(Path dir) throws IOException {
        this(dir, "s3", PLUGIN);
    }

    /**
     * Copies a built plugin into a plugins directory.
     *
     * @param dir test directory
     * @param name plugin name, the directory below plugins
     * @param plugin built plugin directory
     * @throws IOException when the plugin cannot be copied
     */
    S3PluginSupport(Path dir, String name, Path plugin) throws IOException {
        plugins = Files.createDirectories(dir.resolve("plugins"));
        Path s3 = Files.createDirectories(plugins.resolve(name));
        try (Stream<Path> jars = Files.list(plugin)) {
            for (Path jar : jars.toList()) {
                Files.copy(jar, s3.resolve(jar.getFileName().toString()));
            }
        }
    }

    /**
     * Opens a data directory.
     *
     * @param config server configuration with the data directory
     * @return registry
     */
    StoreRegistry open(AppConfig config) {
        return open(config, "directory");
    }

    /**
     * Opens a data directory with catalogs.
     *
     * @param config server configuration with the data directory
     * @param catalogs comma separated catalog providers
     * @return registry
     */
    StoreRegistry open(AppConfig config, String catalogs) {
        Map<String, String> sys = Map.copyOf(properties);
        ConfigSource source = new ConfigSource(sys::get, new Properties(), environment, null);
        return StoreRegistry.open(config, new Configuration(), "localhost",
                new PluginSettings(plugins, null, catalogs, false), source, List.of());
    }
}
