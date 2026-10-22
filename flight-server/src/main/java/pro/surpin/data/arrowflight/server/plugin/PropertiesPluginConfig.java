package pro.surpin.data.arrowflight.server.plugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * Configuration of one plugin namespace, read from the server's sources. A key {@code k} of
 * namespace {@code ns} is looked up as: its legacy aliases (each as a system property, then in
 * {@code arrowflight.properties}), {@code arrowflight.ns.k}, {@code ns.k}, {@code k} in
 * {@code etc/plugins/ns.properties}, then the environment: {@code FLIGHT_NS_K} and the upper
 * case aliases. Secret keys are never read from a property; any property form of one is a
 * startup error.
 */
public final class PropertiesPluginConfig implements PluginConfig {

    private final String namespace;
    private final ConfigSource source;
    private final Map<String, ConfigKey> keys = new LinkedHashMap<>();

    /**
     * Creates the configuration of a namespace.
     *
     * @param namespace namespace, the provider name
     * @param source configuration source
     * @param declared keys the provider declares; undeclared keys are read without aliases
     */
    public PropertiesPluginConfig(String namespace, ConfigSource source, List<ConfigKey> declared) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.source = Objects.requireNonNull(source, "source");
        for (ConfigKey key : declared) {
            keys.put(key.name(), key);
        }
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public Optional<String> get(String key) {
        ConfigKey declared = keys.get(key);
        if (declared != null && declared.secret()) {
            throw new IllegalArgumentException(namespace + "." + key
                    + " is a secret: read it with secret()");
        }
        Optional<String> value = lookup(key, declared);
        if (value.isEmpty() && declared != null && declared.defaultValue() != null) {
            return Optional.of(declared.defaultValue());
        }
        return value;
    }

    @Override
    public Optional<String> secret(String key) {
        ConfigKey declared = keys.get(key);
        requireNotAProperty(key, declared);
        List<String> environment = new ArrayList<>();
        environment.add(ConfigKey.environmentName(namespace, key));
        if (declared != null) {
            declared.legacyAliases().stream().filter(PropertiesPluginConfig::isEnvironmentName)
                    .forEach(environment::add);
        }
        for (String name : environment) {
            Optional<String> value = source.environment(name);
            if (value.isPresent()) {
                return value;
            }
        }
        Optional<String> file = lookup(key + "File", keys.get(key + "File"));
        if (file.isPresent()) {
            return Optional.of(readSecretFile(Path.of(file.get()), namespace + "." + key + "File"));
        }
        return Optional.empty();
    }

    /**
     * Refuses every declared secret key that is set as a property, so that a credential passed
     * the wrong way stops the server even when the code never asks for it.
     *
     * @throws IllegalArgumentException naming the property, never its value
     */
    public void requireNoSecretProperties() {
        for (ConfigKey key : keys.values()) {
            if (key.secret()) {
                requireNotAProperty(key.name(), key);
            }
        }
    }

    /**
     * Lists the property names of a key, in lookup order.
     *
     * @param key key name
     * @param declared declared key or null
     * @return property names (not environment variables)
     */
    List<String> propertyNames(String key, ConfigKey declared) {
        List<String> names = new ArrayList<>();
        if (declared != null) {
            declared.legacyAliases().stream().filter(alias -> !isEnvironmentName(alias))
                    .forEach(names::add);
        }
        names.add("arrowflight." + namespace + "." + key);
        names.add(namespace + "." + key);
        return names;
    }

    /**
     * Looks a plain key up in every source.
     *
     * @param key key name
     * @param declared declared key or null
     * @return trimmed value, empty when unset
     */
    private Optional<String> lookup(String key, ConfigKey declared) {
        Optional<String> value = source.lookup(propertyNames(key, declared), List.of());
        if (value.isPresent()) {
            return value;
        }
        value = source.pluginFileProperty(namespace, key);
        if (value.isPresent()) {
            return value;
        }
        List<String> environment = new ArrayList<>();
        environment.add(ConfigKey.environmentName(namespace, key));
        if (declared != null) {
            declared.legacyAliases().stream().filter(PropertiesPluginConfig::isEnvironmentName)
                    .forEach(environment::add);
        }
        return source.lookup(List.of(), environment);
    }

    /**
     * Refuses a secret set as a property.
     *
     * @param key key name
     * @param declared declared key or null
     */
    private void requireNotAProperty(String key, ConfigKey declared) {
        for (String name : propertyNames(key, declared)) {
            if (source.property(name).isPresent()) {
                throw new IllegalArgumentException(refusal(key, name));
            }
        }
        if (source.pluginFileProperty(namespace, key).isPresent()) {
            throw new IllegalArgumentException(refusal(key, key + " in " + namespace + ".properties"));
        }
    }

    /**
     * Words the refusal of a secret property.
     *
     * @param key key name
     * @param where property it was found in
     * @return message without the value
     */
    private String refusal(String key, String where) {
        return "The " + namespace + " secret " + key + " is not read from properties (" + where
                + "): -D values show in the process list and properties files are shared. Set "
                + ConfigKey.environmentName(namespace, key) + " in the environment or "
                + namespace + "." + key + "File to a file only this user can read";
    }

    /**
     * Reads a secret from a file that other users cannot read.
     *
     * @param file secret file
     * @param key key that named it, for errors
     * @return trimmed content
     */
    private static String readSecretFile(Path file, String key) {
        try {
            PosixFileAttributeView posix = Files.getFileAttributeView(file, PosixFileAttributeView.class);
            if (posix != null
                    && posix.readAttributes().permissions().contains(PosixFilePermission.OTHERS_READ)) {
                throw new IllegalArgumentException(key + " " + file + " is readable by every user;"
                        + " chmod o-r it");
            }
            return Files.readString(file, StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + key + " " + file, e);
        }
    }

    /**
     * Tells whether an alias names an environment variable.
     *
     * @param alias alias
     * @return true for an upper case name
     */
    private static boolean isEnvironmentName(String alias) {
        return alias.matches("[A-Z][A-Z0-9_]*");
    }

    @Override
    public String toString() {
        return "PropertiesPluginConfig[" + namespace + "]";
    }
}
