package pro.surpin.data.arrowflight.plugin.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;

/**
 * Opens S3 buckets ({@code s3://}, {@code s3a://}, {@code s3n://}) as stores.
 */
public final class S3DataStoreProvider implements DataStoreProvider {

    /** Keys of the s3 namespace: the store settings and the names of further stores. */
    static final List<ConfigKey> CONFIG_KEYS = keys();
    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataStoreProvider.class);

    @Override
    public String name() {
        return "s3";
    }

    @Override
    public Set<String> schemes() {
        return Set.of("s3", "s3a", "s3n");
    }

    @Override
    public List<ConfigKey> configKeys() {
        return CONFIG_KEYS;
    }

    /**
     * Opens the store of a root. A catalog root below the root of a named store
     * ({@code arrowflight.s3.stores}) opens that store, at its configured root and with its own
     * endpoint and credentials; the data directory and every other root use the main settings.
     *
     * @param context what the server hands the provider
     * @param root root URI
     * @param use what the store is for
     * @return open store
     */
    @Override
    public DataStore open(StoreContext context, String root, StoreUse use) {
        S3Uri location = S3Uri.parse(root);
        // Read even for the data directory, so that a broken store list stops the start.
        List<S3NamedStores.Store> stores = S3NamedStores.from(context.config());
        Optional<S3NamedStores.Store> named = use == StoreUse.DATA_DIRECTORY ? Optional.empty()
                : S3NamedStores.holding(stores, location);
        String storeRoot = named.map(n -> n.root().uri()).orElse(root);
        String configName = named.map(S3NamedStores.Store::name).orElse("");
        S3Settings settings = S3Settings.from(
                named.map(S3NamedStores.Store::config).orElse(context.config()),
                context.ioParallelism(), true);
        if (named.isPresent() && !settings.hasCredentials()) {
            throw new IllegalArgumentException("S3 store " + configName + " without credentials:"
                    + " set FLIGHT_S3_" + configName.toUpperCase(Locale.ROOT) + "_ACCESS_KEY and"
                    + " FLIGHT_S3_" + configName.toUpperCase(Locale.ROOT) + "_SECRET_KEY or s3."
                    + configName + ".credentialsFile");
        }
        settings.requireUsable();
        context.redactor().add(settings.secretKey());
        context.redactor().add(settings.sessionToken());
        String node = context.nodeName();
        List<String> ignored = S3Settings.awsEnvironmentVariables(System.getenv());
        if (!ignored.isEmpty()) {
            LOGGER.warn("node={} s3=awsEnvironmentIgnored variables={} (this server reads only"
                    + " FLIGHT_S3_* and its own settings)", node, ignored);
        }
        if (settings.useSsl() && !settings.verifyTls()) {
            LOGGER.warn("node={} s3=tlsNotVerified endpoint={} (s3VerifyTls=false: any certificate"
                    + " is accepted)", node, settings.endpointUri());
        }
        S3Client client = S3DataStore.createClient(settings);
        LOGGER.info("node={} s3=open store={} dataDir={} endpoint={} region={} urlStyle={} tls={}"
                + " caFile={} listingTtlSeconds={} maxConnections={}", node,
                configName.isEmpty() ? "main" : configName, S3Uri.parse(storeRoot).uri(),
                settings.endpointUri(), settings.region(), settings.urlStyle(), settings.useSsl(),
                settings.caFile(), settings.listingTtlSeconds(), settings.maxConnections());
        return new S3DataStore(storeRoot, settings, client, System::nanoTime,
                context.threadFactory("s3-listing"), node, true, configName);
    }

    /**
     * Declares the keys of the s3 namespace.
     *
     * @return the store settings plus {@code stores}
     */
    private static List<ConfigKey> keys() {
        List<ConfigKey> keys = new ArrayList<>(S3Settings.CONFIG_KEYS);
        keys.add(ConfigKey.of(S3NamedStores.STORES_KEY, "Comma separated names of further S3"
                + " stores for catalog roots, each configured below s3.<name>. (root, endpoint,"
                + " credentials, ...)", null));
        return List.copyOf(keys);
    }
}
