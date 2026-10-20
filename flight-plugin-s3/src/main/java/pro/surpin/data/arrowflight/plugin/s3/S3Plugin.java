package pro.surpin.data.arrowflight.plugin.s3;

import java.util.List;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

/**
 * The s3 plugin: S3 buckets ({@code s3://}, {@code s3a://}, {@code s3n://}) as data directories,
 * listed and footer-read with the AWS SDK v2 in this plugin's own class loader; DuckDB reads the
 * data through httpfs, set up by the server from this plugin's typed contribution.
 */
public final class S3Plugin implements Plugin {

    @Override
    public List<DataStoreProvider> dataStoreProviders() {
        return List.of(new S3DataStoreProvider());
    }
}
