package pro.surpin.data.arrowflight.plugin.lakefs;

import java.util.List;
import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

/**
 * The lakefs plugin: lakeFS repositories ({@code lakefs://repo/ref/prefix}) as data directories,
 * read through the lakeFS S3 gateway with the s3 plugin's store; branches are pinned to a commit
 * id per query plan.
 */
public final class LakeFsPlugin implements Plugin {

    @Override
    public List<DataStoreProvider> dataStoreProviders() {
        return List.of(new LakeFsDataStoreProvider());
    }
}
