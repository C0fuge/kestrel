package pro.surpin.data.arrowflight.server.plugin;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * Where configuration values come from, in the server's usual order: system properties, then
 * {@code arrowflight.properties} on the class path, for each name in turn, then environment
 * variables. Blank values count as unset. Plugin namespaces may also have a file
 * {@code <configDir>/<ns>.properties}.
 */
public final class ConfigSource {

    private static final String CONFIG_RESOURCE = "arrowflight.properties";

    private final UnaryOperator<String> systemProperties;
    private final Properties properties;
    private final Map<String, String> environment;
    private final Path pluginConfigDir;
    private final Map<String, Properties> pluginFiles = new ConcurrentHashMap<>();

    /**
     * Creates a source.
     *
     * @param systemProperties reads a system property
     * @param properties the properties file
     * @param environment process environment
     * @param pluginConfigDir directory of {@code <ns>.properties} files, or null
     */
    public ConfigSource(UnaryOperator<String> systemProperties, Properties properties,
            Map<String, String> environment, Path pluginConfigDir) {
        this.systemProperties = Objects.requireNonNull(systemProperties, "systemProperties");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.environment = Map.copyOf(environment);
        this.pluginConfigDir = pluginConfigDir;
    }

    /**
     * Reads the server's own sources: system properties, {@code arrowflight.properties} and the
     * environment, without a plugin configuration directory.
     *
     * @return source
     */
    public static ConfigSource system() {
        return new ConfigSource(System::getProperty, loadProperties(), System.getenv(), null);
    }

    /**
     * Returns this source with a plugin configuration directory.
     *
     * @param directory directory of {@code <ns>.properties} files, or null
     * @return source
     */
    public ConfigSource withPluginConfigDir(Path directory) {
        return new ConfigSource(systemProperties, properties, environment, directory);
    }

    /**
     * Looks up names in order: each as a system property and then in the properties file; then
     * the environment variables in order.
     *
     * @param names property names
     * @param environmentNames environment variable names
     * @return trimmed value, empty when unset
     */
    public Optional<String> lookup(List<String> names, List<String> environmentNames) {
        for (String name : names) {
            Optional<String> value = property(name);
            if (value.isPresent()) {
                return value;
            }
        }
        for (String name : environmentNames) {
            Optional<String> value = environment(name);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    /**
     * Reads a system property, then the same name in the properties file.
     *
     * @param name property name
     * @return trimmed value, empty when unset
     */
    public Optional<String> property(String name) {
        Optional<String> value = clean(systemProperties.apply(name));
        return value.isPresent() ? value : clean(properties.getProperty(name));
    }

    /**
     * Reads a system property only.
     *
     * @param name property name
     * @return trimmed value, empty when unset
     */
    public Optional<String> systemProperty(String name) {
        return clean(systemProperties.apply(name));
    }

    /**
     * Reads the properties file only.
     *
     * @param name property name
     * @return trimmed value, empty when unset
     */
    public Optional<String> fileProperty(String name) {
        return clean(properties.getProperty(name));
    }

    /**
     * Reads an environment variable.
     *
     * @param name variable name
     * @return trimmed value, empty when unset
     */
    public Optional<String> environment(String name) {
        return clean(environment.get(name));
    }

    /**
     * Reads a key of a namespace's own file {@code <configDir>/<ns>.properties}.
     *
     * @param namespace namespace
     * @param key key name
     * @return trimmed value, empty when unset or without such a file
     */
    public Optional<String> pluginFileProperty(String namespace, String key) {
        if (pluginConfigDir == null) {
            return Optional.empty();
        }
        Properties file = pluginFiles.computeIfAbsent(namespace, this::loadPluginFile);
        return clean(file.getProperty(key));
    }

    /**
     * Returns the process environment this source reads.
     *
     * @return environment
     */
    public Map<String, String> environment() {
        return environment;
    }

    /**
     * Loads a namespace's file.
     *
     * @param namespace namespace
     * @return its properties, empty without a file
     */
    private Properties loadPluginFile(String namespace) {
        Properties result = new Properties();
        Path file = pluginConfigDir.resolve(namespace + ".properties");
        if (Files.isRegularFile(file)) {
            try (InputStream input = Files.newInputStream(file)) {
                result.load(input);
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read " + file, e);
            }
        }
        return result;
    }

    /**
     * Turns a blank value into empty and trims the others.
     *
     * @param value value or null
     * @return trimmed value
     */
    private static Optional<String> clean(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }

    /**
     * Loads {@code arrowflight.properties} from the class path.
     *
     * @return properties, empty without the resource
     */
    static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = ConfigSource.class.getClassLoader()
                .getResourceAsStream(CONFIG_RESOURCE)) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + CONFIG_RESOURCE, e);
        }
        return properties;
    }
}
