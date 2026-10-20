package pro.surpin.data.arrowflight.spi.storage;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens stores for the URI schemes it names.
 */
public interface DataStoreProvider {

    /**
     * Names the provider; also the namespace of its configuration keys.
     *
     * @return provider name, for example "s3"
     */
    String name();

    /**
     * Names the URI schemes this provider opens, lower case.
     *
     * @return schemes, for example {@code s3}, {@code s3a} and {@code s3n}
     */
    Set<String> schemes();

    /**
     * Names the URI schemes the stores of this provider give DuckDB (roots and secret scopes),
     * lower case; the server refuses any other. A provider that reads through another protocol
     * (lakeFS through its S3 gateway) names that protocol's scheme here.
     *
     * @return schemes DuckDB reads with, {@link #schemes()} by default
     */
    default Set<String> duckDbSchemes() {
        return schemes();
    }

    /**
     * Declares the configuration keys of this provider, with their legacy aliases.
     *
     * @return configuration keys
     */
    List<ConfigKey> configKeys();

    /**
     * Opens a store.
     *
     * @param context what the server hands the provider
     * @param root root URI, with one of {@link #schemes()}
     * @param use what the store is opened for
     * @return open store; the server closes it
     * @throws IOException when the store cannot be opened
     * @throws IllegalArgumentException when the configuration cannot reach the root
     */
    DataStore open(StoreContext context, String root, StoreUse use) throws IOException;
}
