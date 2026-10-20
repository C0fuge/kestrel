package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbExtensions;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupValidator;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.DefaultStoreContext;
import pro.surpin.data.arrowflight.server.plugin.PluginDescriptor;
import pro.surpin.data.arrowflight.server.plugin.PluginManager;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.plugin.PropertiesPluginConfig;
import pro.surpin.data.arrowflight.server.plugin.ThreadContext;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.StoreUse;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * The stores this server has open, chosen by the scheme of the data directory: a plugin that
 * claims the scheme in its descriptor, else a built-in provider, else, for a scheme only a known
 * plugin provides ({@code s3}), a startup error naming that plugin, else the cluster's Hadoop
 * file system (HDFS, local paths and every other Hadoop scheme, as always). Plugins that the
 * configuration does not need are never loaded. The DuckDB setup of the stores is checked and
 * written out when the registry opens, so that a refused contribution stops the server at once.
 */
public final class StoreRegistry implements Stores, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StoreRegistry.class);
    /** A URI scheme followed by "//", which a Windows drive letter is not. */
    private static final Pattern SCHEME = Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*)://");
    /** Schemes that only a plugin provides, to the plugin's name. */
    private static final Map<String, String> PLUGIN_SCHEMES = Map.of(
            "s3", "s3", "s3a", "s3", "s3n", "s3");

    private final DataStore dataDirectory;
    private final String provider;
    private final PluginManager plugins;
    private final SecretRedactor redactor;
    private final DuckDbSetupPlan duckDbSetup;

    /**
     * Creates a registry from opened parts.
     *
     * @param dataDirectory store of the data directory
     * @param provider name of its provider
     * @param plugins plugin manager
     * @param redactor redactor of the stores' secrets
     * @param duckDbSetup checked DuckDB setup of the stores
     */
    private StoreRegistry(DataStore dataDirectory, String provider, PluginManager plugins,
            SecretRedactor redactor, DuckDbSetupPlan duckDbSetup) {
        this.dataDirectory = dataDirectory;
        this.provider = provider;
        this.plugins = plugins;
        this.redactor = redactor;
        this.duckDbSetup = duckDbSetup;
    }

    /**
     * Opens the store of the configured data directory with the server's own configuration.
     *
     * @param config server configuration
     * @param hadoopConfig Hadoop configuration
     * @param storageHost storage host colocated with this server
     * @return registry
     */
    public static StoreRegistry open(AppConfig config, Configuration hadoopConfig, String storageHost) {
        ConfigSource source = ConfigSource.system();
        return open(config, hadoopConfig, storageHost, PluginSettings.from(source), source,
                builtInProviders());
    }

    /**
     * Returns the storage providers built into the server.
     *
     * @return built-in providers besides the Hadoop file system: none, S3 is the s3 plugin
     */
    static List<DataStoreProvider> builtInProviders() {
        return List.of();
    }

    /**
     * Opens the store of the configured data directory.
     *
     * @param config server configuration
     * @param hadoopConfig Hadoop configuration
     * @param storageHost storage host colocated with this server
     * @param settings plugin settings
     * @param source configuration source
     * @param builtIns built-in storage providers
     * @return registry
     * @throws IllegalArgumentException when the configuration cannot be served
     * @throws IllegalStateException when a needed plugin cannot be loaded
     */
    public static StoreRegistry open(AppConfig config, Configuration hadoopConfig,
            String storageHost, PluginSettings settings, ConfigSource source,
            List<DataStoreProvider> builtIns) {
        if (!PluginSettings.DIRECTORY_CATALOG.equals(settings.catalog())) {
            throw new IllegalArgumentException("catalog=" + settings.catalog() + " is not available"
                    + " in this server version: only the directory catalog is");
        }
        String node = LogUtil.node();
        String dataDir = config.dataDir();
        ClassLoader server = StoreRegistry.class.getClassLoader();
        PluginManager plugins = new PluginManager(settings.pluginsDir(),
                settings.requireChecksums(), server, node);
        SecretRedactor redactor = new SecretRedactor();
        ConfigSource pluginSource = source.withPluginConfigDir(settings.pluginConfigDir());
        DataStore store = null;
        try {
            String scheme = schemeOf(dataDir);
            DuckDbSetupValidator.Contribution contribution;
            String providerName;
            Optional<Map.Entry<PluginManager.LoadedPlugin, DataStoreProvider>> fromPlugin =
                    Optional.empty();
            if (scheme != null) {
                if (claimedBySharingPlugin(plugins, scheme)) {
                    // Hadoop registers its file systems once per JVM, with the context class
                    // loader of the first caller: let that be the server's.
                    try (ThreadContext ignored = ThreadContext.of(server)) {
                        FileSystem.getLocal(hadoopConfig);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                fromPlugin = plugins.storageProvider(scheme);
            }
            if (fromPlugin.isPresent()) {
                PluginManager.LoadedPlugin plugin = fromPlugin.get().getKey();
                DataStoreProvider chosen = fromPlugin.get().getValue();
                store = openSpi(chosen, plugin.loader(), dataDir, storageHost, config, redactor,
                        pluginSource, node);
                providerName = chosen.name();
                contribution = new DuckDbSetupValidator.Contribution(chosen.name(),
                        chosen.schemes(), store.duckDbRoots(), store::contributeDuckDb);
            } else {
                DataStoreProvider builtIn = scheme == null ? null : builtIns.stream()
                        .filter(p -> p.schemes().contains(scheme)).findFirst().orElse(null);
                if (builtIn != null) {
                    store = openSpi(builtIn, null, dataDir, storageHost, config, redactor,
                            pluginSource, node);
                    providerName = builtIn.name();
                    contribution = new DuckDbSetupValidator.Contribution(builtIn.name(),
                            builtIn.schemes(), store.duckDbRoots(), store::contributeDuckDb);
                } else if (scheme != null && PLUGIN_SCHEMES.containsKey(scheme)) {
                    throw missingPlugin(dataDir, PLUGIN_SCHEMES.get(scheme), plugins);
                } else {
                    store = HadoopDataStoreProvider.open(dataDir, hadoopConfig,
                            config.ioFileBufferSize(), storageHost);
                    providerName = "hadoop";
                    contribution = DuckDbSetupValidator.Contribution.ofStore("hadoop", store);
                }
            }
            DuckDbSetupPlan plan = new DuckDbSetupValidator(new DuckDbExtensions(source), redactor)
                    .plan(contribution, List.of(), List.of());
            plugins.logUnused();
            LOGGER.info("node={} storage=open provider={} dataDir={} roots={} duckDbSetup={}",
                    node, providerName, dataDir, plan.roots(), plan);
            return new StoreRegistry(store, providerName, plugins, redactor, plan);
        } catch (RuntimeException e) {
            closeQuietly(store);
            plugins.close();
            throw e;
        }
    }

    /**
     * Returns the store of the data directory.
     *
     * @return store
     */
    public DataStore dataDirectory() {
        return dataDirectory;
    }

    /**
     * Names the provider of the data directory's store.
     *
     * @return provider name ("hadoop" for the built-in Hadoop file system)
     */
    public String provider() {
        return provider;
    }

    /**
     * Returns the checked DuckDB setup of the stores.
     *
     * @return setup plan
     */
    public DuckDbSetupPlan duckDbSetup() {
        return duckDbSetup;
    }

    /**
     * Returns the plugin manager.
     *
     * @return plugin manager
     */
    public PluginManager plugins() {
        return plugins;
    }

    /**
     * Returns the redactor of the stores' secrets.
     *
     * @return redactor
     */
    public SecretRedactor redactor() {
        return redactor;
    }

    /**
     * Returns the roots DuckDB may read.
     *
     * @return readable roots
     */
    public List<String> readableRoots() {
        return duckDbSetup.roots();
    }

    @Override
    public Located locate(String absoluteUri) throws IOException {
        Objects.requireNonNull(absoluteUri, "absoluteUri");
        String best = null;
        for (String root : dataDirectory.duckDbRoots()) {
            String prefix = root.endsWith("/") ? root : root + "/";
            if ((absoluteUri.startsWith(prefix) || absoluteUri.equals(root))
                    && (best == null || prefix.length() > best.length())) {
                best = prefix;
            }
        }
        if (best == null) {
            throw new IOException("No open store holds " + absoluteUri);
        }
        String relative = absoluteUri.length() <= best.length() ? "" : absoluteUri.substring(best.length());
        return new Located(dataDirectory, relative);
    }

    @Override
    public void close() {
        closeQuietly(dataDirectory);
        plugins.close();
    }

    /**
     * Returns the scheme of a URI.
     *
     * @param uri URI or path
     * @return lower case scheme, null for a plain path
     */
    static String schemeOf(String uri) {
        if (uri == null) {
            return null;
        }
        Matcher m = SCHEME.matcher(uri.trim());
        return m.find() ? m.group(1).toLowerCase(Locale.ROOT) : null;
    }

    /**
     * Opens a store of an SPI provider with its plugin's context class loader.
     *
     * @param provider provider
     * @param loader plugin loader, null for a built-in provider
     * @param root root URI
     * @param storageHost storage host
     * @param config server configuration
     * @param redactor redactor of the stores' secrets
     * @param source configuration source
     * @param node this node
     * @return store as the server uses it
     */
    private static DataStore openSpi(DataStoreProvider provider, ClassLoader loader, String root,
            String storageHost, AppConfig config, SecretRedactor redactor, ConfigSource source,
            String node) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            PropertiesPluginConfig pluginConfig = new PropertiesPluginConfig(provider.name(), source,
                    provider.configKeys());
            pluginConfig.requireNoSecretProperties();
            DefaultStoreContext context = new DefaultStoreContext(pluginConfig, node, storageHost,
                    config.ioParallelism(), redactor, loader);
            return new SpiDataStore(provider.open(context, root.trim(), StoreUse.DATA_DIRECTORY),
                    loader, provider.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Tells whether the plugin that claims a scheme shares the server's Hadoop.
     *
     * @param plugins plugin manager
     * @param scheme scheme
     * @return true when such a plugin claims the scheme
     */
    private static boolean claimedBySharingPlugin(PluginManager plugins, String scheme) {
        for (PluginDescriptor descriptor : plugins.present().values()) {
            if (descriptor.shareHadoop() && descriptor.storageSchemes().contains(scheme)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds the startup error for a data directory whose plugin is missing.
     *
     * @param dataDir data directory
     * @param plugin plugin name
     * @param plugins plugin manager
     * @return exception to throw
     */
    private static IllegalArgumentException missingPlugin(String dataDir, String plugin,
            PluginManager plugins) {
        String where = plugins.pluginsDir() == null
                ? "no plugins directory is configured (arrowflight.plugins.dir or FLIGHT_PLUGINS_DIR)"
                : "no usable " + plugins.pluginsDir().resolve(plugin) + " directory"
                        + plugins.problem(plugin).map(p -> " (" + p + ")").orElse("")
                        + "; set arrowflight.plugins.dir or FLIGHT_PLUGINS_DIR";
        return new IllegalArgumentException("dataDir " + dataDir + " needs the " + plugin
                + " plugin, but " + where);
    }

    /**
     * Closes a store, logging a failure.
     *
     * @param store store or null
     */
    private static void closeQuietly(DataStore store) {
        if (store == null) {
            return;
        }
        try {
            store.close();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("node={} storage=closeFailed error={}", LogUtil.node(), e.toString());
        }
    }

    /**
     * Lists the stores' roots, for logs and tests.
     *
     * @return roots of the data directory's store
     */
    List<String> roots() {
        return new ArrayList<>(dataDirectory.duckDbRoots());
    }
}
