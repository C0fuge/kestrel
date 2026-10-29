package pro.surpin.data.arrowflight.plugin.jdbc;

import java.util.Map;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * Plugin configuration from two maps: values and secrets (the server reads secrets from the
 * environment or a file only).
 *
 * @param values properties below {@code arrowflight.jdbc.}
 * @param secrets secrets by key
 */
record MapConfig(Map<String, String> values, Map<String, String> secrets) implements PluginConfig {

    @Override
    public String namespace() {
        return "jdbc";
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(values.get(key));
    }

    @Override
    public Optional<String> secret(String key) {
        return Optional.ofNullable(secrets.get(key));
    }
}
