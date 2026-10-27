package pro.surpin.data.arrowflight.plugin.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    /** Linux setting that lets a new connection take the port of one in TIME_WAIT. */
    private static final Path TCP_TW_REUSE = Path.of("/proc/sys/net/ipv4/tcp_tw_reuse");
    /** Linux setting with the first and last local port of outgoing connections. */
    private static final Path PORT_RANGE = Path.of("/proc/sys/net/ipv4/ip_local_port_range");
    /** Seconds Linux keeps a closed connection's port in TIME_WAIT. */
    private static final int TIME_WAIT_SECONDS = 60;
    private static final AtomicBoolean PORTS_CHECKED = new AtomicBoolean();

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
        if (PORTS_CHECKED.compareAndSet(false, true)) {
            ephemeralPortWarning(settings.connectionCaching(), readSetting(TCP_TW_REUSE),
                    readSetting(PORT_RANGE))
                    .ifPresent(warning -> LOGGER.warn("node={} s3=ephemeralPorts {}", node, warning));
        }
        S3Client client = S3DataStore.createClient(settings);
        LOGGER.info("node={} s3=open store={} dataDir={} endpoint={} region={} urlStyle={} tls={}"
                + " caFile={} listingTtlSeconds={} maxConnections={} connectionCaching={}", node,
                configName.isEmpty() ? "main" : configName, S3Uri.parse(storeRoot).uri(),
                settings.endpointUri(), settings.region(), settings.urlStyle(), settings.useSsl(),
                settings.caFile(), settings.listingTtlSeconds(), settings.maxConnections(),
                settings.connectionCaching());
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

    /**
     * Describes the limit the host's port settings put on DuckDB's S3 reads. Without connection
     * caching, at 4 or more DuckDB threads httpfs opens a new connection for about every range
     * read and closes it afterwards; each closed connection keeps its local port for 60 s, so
     * without {@code tcp_tw_reuse=1} the port range caps the reads per second (item 0g). Only
     * checked when {@code connectionCaching=false}.
     *
     * @param connectionCaching whether DuckDB keeps its S3 connections
     * @param twReuse value of net.ipv4.tcp_tw_reuse, or null when it cannot be read
     * @param portRange value of net.ipv4.ip_local_port_range, or null when it cannot be read
     * @return the warning, or empty when connections are kept, ports are reused or the settings
     *         are unknown
     */
    static Optional<String> ephemeralPortWarning(boolean connectionCaching, String twReuse,
            String portRange) {
        if (connectionCaching || twReuse == null || "1".equals(twReuse.trim())) {
            return Optional.empty();
        }
        String ports = "the local port range";
        String rate = "";
        String[] range = portRange == null ? new String[0] : portRange.trim().split("\\s+");
        if (range.length == 2) {
            try {
                int count = Integer.parseInt(range[1]) - Integer.parseInt(range[0]) + 1;
                ports = count + " local ports (ip_local_port_range " + range[0] + "-" + range[1] + ")";
                rate = ", about " + count / TIME_WAIT_SECONDS + " per second";
            } catch (NumberFormatException e) {
                // Unusual format: the warning goes without the numbers.
            }
        }
        return Optional.of("net.ipv4.tcp_tw_reuse=" + twReuse.trim() + ": with s3 connectionCaching=false DuckDB opens a"
                + " new S3 connection for most range reads at 4+ threads and each closed one holds its port"
                + " " + TIME_WAIT_SECONDS + " s, so " + ports + " bound the reads per endpoint" + rate
                + "; beyond that queries fail \"Could not establish connection\". Set"
                + " arrowflight.s3.connectionCaching=true (the default), or"
                + " net.ipv4.tcp_tw_reuse=1 in the server's network namespace (and widen"
                + " ip_local_port_range).");
    }

    /**
     * Reads a one-line kernel setting.
     *
     * @param file file below /proc/sys
     * @return its content, or null when it cannot be read (not Linux, no access)
     */
    private static String readSetting(Path file) {
        try {
            return Files.readString(file).trim();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }
}
