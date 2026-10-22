package pro.surpin.data.arrowflight.plugin.fixture;

import java.util.List;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;

/**
 * Opens {@link FixtureStore}s.
 */
public final class FixtureStoreProvider implements DataStoreProvider {

    @Override
    public String name() {
        return "fixture";
    }

    @Override
    public Set<String> schemes() {
        return Set.of("fixture");
    }

    @Override
    public List<ConfigKey> configKeys() {
        return List.of(ConfigKey.of("greeting", "A value to read back", "hello"));
    }

    @Override
    public DataStore open(StoreContext context, String root, StoreUse use) {
        return new FixtureStore(context, root);
    }
}
