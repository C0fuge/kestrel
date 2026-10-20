package pro.surpin.data.arrowflight.plugin.fixture;

import java.util.List;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

/**
 * Test plugin: one storage provider for the scheme "fixture".
 */
public final class FixturePlugin implements Plugin {

    @Override
    public List<DataStoreProvider> dataStoreProviders() {
        return List.of(new FixtureStoreProvider());
    }
}
