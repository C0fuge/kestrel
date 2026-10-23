package pro.surpin.data.arrowflight.plugin.lakefs;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.plugin.s3.S3DataStore;
import pro.surpin.data.arrowflight.plugin.s3.S3Settings;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Opens lakeFS repositories ({@code lakefs://repository/ref/prefix}) as stores. Configuration
 * namespace {@code lakefs}: the s3 plugin's keys without their legacy aliases ({@code endpoint}
 * is the lakeFS server, whose S3 gateway is at its root; path style by default), plus
 * {@code apiEndpoint}, {@code pinBranches}, {@code pinTtlMillis} and {@code apiTimeoutMillis}.
 * Credentials only as {@code FLIGHT_LAKEFS_ACCESS_KEY} / {@code FLIGHT_LAKEFS_SECRET_KEY} or
 * {@code credentialsFile}.
 */
public final class LakeFsDataStoreProvider implements DataStoreProvider {

    /** Default time one branch resolution is shared by the listings of a query plan. */
    public static final int DEFAULT_PIN_TTL_MILLIS = 1000;

    /** Configuration keys of the lakefs namespace. */
    public static final List<ConfigKey> CONFIG_KEYS = configKeys(S3Settings.CONFIG_KEYS);

    private static final Logger LOGGER = LoggerFactory.getLogger(LakeFsDataStoreProvider.class);

    @Override
    public String name() {
        return "lakefs";
    }

    @Override
    public Set<String> schemes() {
        return Set.of(LakeFsUri.SCHEME);
    }

    /**
     * DuckDB reads pinned commits through the S3 gateway: roots and the secret are s3://.
     *
     * @return {@code s3}
     */
    @Override
    public Set<String> duckDbSchemes() {
        return Set.of("s3");
    }

    @Override
    public List<ConfigKey> configKeys() {
        return CONFIG_KEYS;
    }

    @Override
    public DataStore open(StoreContext context, String root, StoreUse use) {
        LakeFsUri uri = LakeFsUri.parse(root);
        PluginConfig config = context.config();
        S3Settings settings = S3Settings.from(config, context.ioParallelism(), true);
        if (settings.endpoint() == null) {
            throw new IllegalArgumentException("lakefs data directory " + uri.uri()
                    + " needs the lakeFS endpoint (arrowflight.lakefs.endpoint)");
        }
        if (!settings.hasCredentials()) {
            // S3Settings names the s3 plugin's variables; name the lakefs ones.
            throw new IllegalArgumentException("lakefs data directory " + uri.uri()
                    + " without credentials: set FLIGHT_LAKEFS_ACCESS_KEY and"
                    + " FLIGHT_LAKEFS_SECRET_KEY or arrowflight.lakefs.credentialsFile");
        }
        if (settings.caFile() != null && !Files.isReadable(
                Path.of(settings.caFile()))) {
            throw new IllegalArgumentException("arrowflight.lakefs.caFile is not a readable file: "
                    + settings.caFile());
        }
        settings.requireUsable();
        context.redactor().add(settings.secretKey());
        context.redactor().add(settings.sessionToken());
        URI api = URI.create(config.get("apiEndpoint", apiEndpoint(settings.endpoint())));
        boolean pin = config.getBoolean("pinBranches", true);
        int pinTtl = config.getInt("pinTtlMillis", DEFAULT_PIN_TTL_MILLIS);
        int apiTimeout = config.getInt("apiTimeoutMillis", 10_000);
        RefResolver resolver = new HttpRefResolver(api, settings.accessKey(), settings.secretKey(),
                settings.caFile(), Duration.ofMillis(Math.max(1, apiTimeout)));
        S3Client client = S3DataStore.createClient(settings);
        LOGGER.info("node={} lakefs=open dataDir={} gateway={} api={} pinBranches={}"
                + " pinTtlMillis={} tls={} caFile={} listingTtlSeconds={}", context.nodeName(),
                uri.uri(), settings.endpointUri(), api, pin, pinTtl, settings.useSsl(),
                settings.caFile(), settings.listingTtlSeconds());
        return new LakeFsDataStore(uri, settings, client, resolver, pin, pinTtl, System::nanoTime,
                context.threadFactory("lakefs-listing"), context.nodeName());
    }

    /**
     * Derives the REST API base from the server endpoint.
     *
     * @param endpoint lakeFS server, for example {@code http://lakefs:8000}
     * @return {@code <endpoint>/api/v1}
     */
    static String apiEndpoint(String endpoint) {
        String base = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        return base + "/api/v1";
    }

    private static List<ConfigKey> configKeys(List<ConfigKey> s3Keys) {
        List<ConfigKey> keys = new ArrayList<>();
        for (ConfigKey key : s3Keys) {
            keys.add(key.secret()
                    ? ConfigKey.secret(key.name(), key.description())
                    : ConfigKey.of(key.name(), key.description(), key.defaultValue()));
        }
        keys.add(ConfigKey.of("apiEndpoint", "lakeFS REST API base; default <endpoint>/api/v1",
                null));
        keys.add(ConfigKey.of("pinBranches", "Pin a branch to its head commit for each query plan",
                "true"));
        keys.add(ConfigKey.of("pinTtlMillis", "Milliseconds one branch resolution is shared",
                Integer.toString(DEFAULT_PIN_TTL_MILLIS)));
        keys.add(ConfigKey.of("apiTimeoutMillis", "Connect and request timeout of the REST API",
                "10000"));
        return List.copyOf(keys);
    }
}
