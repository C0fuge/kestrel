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
 * Where configuration values come from, in the server's order: system properties (every name in
 * turn), then environment variables, then {@code arrowflight.properties} on the class path (every
 * name in turn), so the bundled file only supplies defaults. Blank values count as unset. Plugin
 * namespaces may also have a file {@code <configDir>/<ns>.properties}, read after the environment
 * and before {@code arrowflight.properties}.
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
     * Looks up names in order: every name as a system property, then the environment variables,
     * then every name in the properties file.
     *
     * @param names property names
     * @param environmentNames environment variable names
     * @return trimmed value, empty when unset
     */
    public Optional<String> lookup(List<String> names, List<String> environmentNames) {
        return firstSystemProperty(names)
                .or(() -> firstEnvironment(environmentNames))
                .or(() -> firstFileProperty(names));
    }

    /**
     * Reads the first of some names that is set as a system property.
     *
     * @param names property names
     * @return trimmed value, empty when none is set
     */
    public Optional<String> firstSystemProperty(List<String> names) {
        for (String name : names) {
            Optional<String> value = systemProperty(name);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    /**
     * Reads the first of some environment variables that is set.
     *
     * @param names variable names
     * @return trimmed value, empty when none is set
     */
    public Optional<String> firstEnvironment(List<String> names) {
        for (String name : names) {
            Optional<String> value = environment(name);
            if (value.isPresent()) {
                return value;
            }
        }
        return Optional.empty();
    }

    /**
     * Reads the first of some names that is set in the properties file.
     *
     * @param names property names
     * @return trimmed value, empty when none is set
     */
    public Optional<String> firstFileProperty(List<String> names) {
        for (String name : names) {
            Optional<String> value = fileProperty(name);
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
