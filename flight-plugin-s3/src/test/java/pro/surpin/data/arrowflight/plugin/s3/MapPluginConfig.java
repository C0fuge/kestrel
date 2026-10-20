package pro.surpin.data.arrowflight.plugin.s3;

import java.util.Map;
import java.util.Optional;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/** A plugin configuration from maps, for tests: values by key name, secrets by key name. */
final class MapPluginConfig implements PluginConfig {

    private final Map<String, String> values;
    private final Map<String, String> secrets;

    MapPluginConfig(Map<String, String> values, Map<String, String> secrets) {
        this.values = values;
        this.secrets = secrets;
    }

    @Override
    public String namespace() {
        return "s3";
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
