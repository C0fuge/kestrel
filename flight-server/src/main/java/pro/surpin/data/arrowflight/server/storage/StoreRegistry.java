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
import pro.surpin.data.arrowflight.server.duckdb.TableSecrets;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.plugin.ConfigSource;
import pro.surpin.data.arrowflight.server.plugin.DefaultStoreContext;
import pro.surpin.data.arrowflight.server.plugin.PluginDescriptor;
import pro.surpin.data.arrowflight.server.plugin.PluginManager;
import pro.surpin.data.arrowflight.server.plugin.PluginSettings;
import pro.surpin.data.arrowflight.server.plugin.PropertiesPluginConfig;
import pro.surpin.data.arrowflight.server.plugin.ThreadContext;
import pro.surpin.data.arrowflight.server.catalog.CatalogLocations;
import pro.surpin.data.arrowflight.server.catalog.CatalogTables;
import pro.surpin.data.arrowflight.server.catalog.DefaultCatalogContext;
import pro.surpin.data.arrowflight.server.catalog.SpiCatalog;
import pro.surpin.data.arrowflight.server.catalog.SpiTableFormat;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableFormatProvider;
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
            "s3", "s3", "s3a", "s3", "s3n", "s3", "lakefs", "lakefs");

    private final DataStore dataDirectory;
    private final String provider;
    private final PluginManager plugins;
    private final SecretRedactor redactor;
    private final DuckDbSetupPlan duckDbSetup;
    private final List<DataStore> extraStores;
    private final CatalogTables catalogTables;
    private final TableSecrets tableSecrets;

    /**
     * Creates a registry from opened parts.
     *
     * @param dataDirectory store of the data directory
     * @param provider name of its provider
     * @param plugins plugin manager
     * @param redactor redactor of the stores' secrets
     * @param duckDbSetup checked DuckDB setup of the stores and catalogs
     * @param extraStores stores opened for catalog roots
     * @param catalogTables open catalogs besides the data directory
     * @param tableSecrets storage credentials the catalogs returned with their tables
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    private StoreRegistry(DataStore dataDirectory, String provider, PluginManager plugins,
            SecretRedactor redactor, DuckDbSetupPlan duckDbSetup, List<DataStore> extraStores,
            CatalogTables catalogTables, TableSecrets tableSecrets) {
        this.dataDirectory = dataDirectory;
        this.provider = provider;
        this.plugins = plugins;
        this.redactor = redactor;
        this.duckDbSetup = duckDbSetup;
        this.extraStores = List.copyOf(extraStores);
        this.catalogTables = catalogTables;
        this.tableSecrets = tableSecrets;
    }

    /**
     * Returns the storage credentials the catalogs returned with their tables (SPI 1.7), which
     * every DuckDB connection installs before it runs a query.
     *
     * @return registry of table secrets
     */
    public TableSecrets tableSecrets() {
        return tableSecrets;
    }

    /**
     * Returns the tables of the configured catalogs besides the data directory.
     *
     * @return catalog tables, empty without catalogs
     */
    public CatalogTables catalogTables() {
        return catalogTables;
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
        String node = LogUtil.node();
        String dataDir = config.dataDir();
        ClassLoader server = StoreRegistry.class.getClassLoader();
        PluginManager plugins = new PluginManager(settings.pluginsDir(),
                settings.requireChecksums(), server, node);
        SecretRedactor redactor = new SecretRedactor();
        ConfigSource pluginSource = source.withPluginConfigDir(settings.pluginConfigDir());
        Opening opening = new Opening(config, hadoopConfig, storageHost, plugins, builtIns, redactor,
                pluginSource, node, server);
        Opened dataDirectory = null;
        List<Opened> extraStores = new ArrayList<>();
        List<CatalogTables.Entry> catalogs = new ArrayList<>();
        TableSecrets tableSecrets = new TableSecrets();
        try {
            dataDirectory = opening.open(dataDir, StoreUse.DATA_DIRECTORY);
            List<DuckDbSetupValidator.Contribution> catalogContributions = new ArrayList<>();
            if (!settings.catalogs().isEmpty()) {
                CatalogStores stores = new CatalogStores(dataDirectory, extraStores, opening);
                // Local catalog roots are read by this node only, unless every node mounts them.
                boolean localRootsShared = source.lookup(
                        List.of("arrowflight.catalog.localRootsShared"),
                        List.of("FLIGHT_CATALOG_LOCAL_ROOTS_SHARED"))
                        .map(Boolean::parseBoolean).orElse(false);
                for (String name : settings.catalogs()) {
                    catalogs.add(openCatalog(name, stores, opening, localRootsShared,
                            tableSecrets));
                }
                stores.freeze();
                for (CatalogTables.Entry entry : catalogs) {
                    for (String root : entry.roots()) {
                        if (stores.holder(root) == null) {
                            throw new IllegalArgumentException("Catalog " + entry.provider()
                                    + " names the readable root " + root + ", which no open store"
                                    + " holds (the catalog must locate its roots when it opens)");
                        }
                    }
                    catalogContributions.add(new DuckDbSetupValidator.Contribution(entry.provider(),
                            schemesOf(entry.roots()), entry.roots(), entry.catalog()::contributeDuckDb));
                }
            }
            List<DuckDbSetupValidator.Contribution> storeContributions = new ArrayList<>();
            for (Opened extra : extraStores) {
                storeContributions.add(extra.contribution());
            }
            DuckDbSetupPlan plan = new DuckDbSetupValidator(new DuckDbExtensions(source), redactor)
                    .plan(dataDirectory.contribution(), storeContributions, catalogContributions);
            plugins.logUnused();
            LOGGER.info("node={} storage=open provider={} dataDir={} roots={} duckDbSetup={}",
                    node, dataDirectory.provider(), dataDir, plan.roots(), plan);
            for (CatalogTables.Entry entry : catalogs) {
                LOGGER.info("node={} catalog=open provider={} roots={} shared={}", node,
                        entry.provider(), entry.roots(), entry.shared());
            }
            List<DataStore> extras = new ArrayList<>();
            for (Opened extra : extraStores) {
                extras.add(extra.store());
                LOGGER.info("node={} storage=openForCatalog provider={} roots={}", node,
                        extra.provider(), extra.store().duckDbRoots());
            }
            return new StoreRegistry(dataDirectory.store(), dataDirectory.provider(), plugins,
                    redactor, plan, extras, new CatalogTables(catalogs), tableSecrets);
        } catch (RuntimeException e) {
            new CatalogTables(catalogs).close();
            for (Opened extra : extraStores) {
                closeQuietly(extra.store());
            }
            closeQuietly(dataDirectory == null ? null : dataDirectory.store());
            plugins.close();
            throw e;
        }
    }

    /**
     * A store opened by the registry.
     *
     * @param store store as the server uses it
     * @param provider provider name ("hadoop" for the Hadoop file system)
     * @param contribution its DuckDB setup, checked when the plan is made
     */
    private record Opened(DataStore store, String provider,
            DuckDbSetupValidator.Contribution contribution) {
    }

    /**
     * Tells whether a local data directory is declared a mount every node reads (NFS and the
     * like): {@code arrowflight.storage.dataDirShared} or {@code FLIGHT_DATA_DIR_SHARED}, default
     * false. A cluster then plans complete answers and spreads files over all nodes as for HDFS;
     * a directory that differs between nodes would give answers that miss files.
     *
     * @param source configuration source
     * @return true when declared shared
     */
    static boolean dataDirShared(ConfigSource source) {
        return source.lookup(List.of("dataDirShared", "arrowflight.storage.dataDirShared"),
                List.of("FLIGHT_DATA_DIR_SHARED")).map(Boolean::parseBoolean).orElse(false);
    }

    /**
     * What opening a store needs.
     *
     * @param config server configuration
     * @param hadoopConfig Hadoop configuration
     * @param storageHost storage host
     * @param plugins plugin manager
     * @param builtIns built-in providers
     * @param redactor redactor of secrets
     * @param source configuration source of the plugins
     * @param node this node
     * @param server the server's class loader
     */
    private record Opening(AppConfig config, Configuration hadoopConfig, String storageHost,
            PluginManager plugins, List<DataStoreProvider> builtIns, SecretRedactor redactor,
            ConfigSource source, String node, ClassLoader server) {

        /**
         * Opens the store of a root: a plugin that claims the scheme, else a built-in provider,
         * else for a plugin-only scheme an error naming the plugin, else the Hadoop file system.
         *
         * @param root root URI or path
         * @param use what the store is for
         * @return opened store
         */
        Opened open(String root, StoreUse use) {
            String scheme = schemeOf(root);
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
                DataStore store = openSpi(chosen, plugin.loader(), root, storageHost, config,
                        redactor, source, node, use);
                return new Opened(store, chosen.name(), new DuckDbSetupValidator.Contribution(
                        chosen.name(), chosen.duckDbSchemes(), store.duckDbRoots(),
                        store::contributeDuckDb));
            }
            DataStoreProvider builtIn = scheme == null ? null : builtIns.stream()
                    .filter(p -> p.schemes().contains(scheme)).findFirst().orElse(null);
            if (builtIn != null) {
                DataStore store = openSpi(builtIn, null, root, storageHost, config, redactor,
                        source, node, use);
                return new Opened(store, builtIn.name(), new DuckDbSetupValidator.Contribution(
                        builtIn.name(), builtIn.schemes(), store.duckDbRoots(),
                        store::contributeDuckDb));
            }
            if (scheme != null && PLUGIN_SCHEMES.containsKey(scheme)) {
                throw missingPlugin(use == StoreUse.DATA_DIRECTORY ? "dataDir " + root
                        : "catalog root " + root, PLUGIN_SCHEMES.get(scheme), plugins);
            }
            DataStore store = HadoopDataStoreProvider.open(root, hadoopConfig,
                    config.ioFileBufferSize(), storageHost,
                    use == StoreUse.DATA_DIRECTORY && dataDirShared(source));
            return new Opened(store, "hadoop",
                    DuckDbSetupValidator.Contribution.ofStore("hadoop", store));
        }
    }

    /**
     * The stores handed to catalogs while they open: the data directory's store, and a new
     * store for every root a catalog locates outside the open ones (until frozen). One store per
     * storage plugin configuration: a second root of the same plugin and configuration name (two
     * S3 secrets with one name) is refused.
     */
    private static final class CatalogStores implements Stores {
        private final Opened dataDirectory;
        private final List<Opened> extras;
        private final Opening opening;
        private boolean frozen;

        /**
         * Creates the stores view.
         *
         * @param dataDirectory the data directory's store
         * @param extras receives the stores opened for catalogs
         * @param opening opens stores
         */
        CatalogStores(Opened dataDirectory, List<Opened> extras, Opening opening) {
            this.dataDirectory = dataDirectory;
            this.extras = extras;
            this.opening = opening;
        }

        /** Refuses to open more stores. */
        synchronized void freeze() {
            frozen = true;
        }

        @Override
        public synchronized Located locate(String absoluteUri) throws IOException {
            String location = CatalogLocations.normalize(absoluteUri);
            Located found = holder(location);
            if (found != null) {
                return found;
            }
            if (frozen) {
                throw new IOException("No open store holds " + absoluteUri);
            }
            // A catalog location without a scheme names a local file (as DuckDB reads it), never
            // a path of the Hadoop default file system (fs.defaultFS = hdfs://...).
            Opened opened = opening.open(location.startsWith("/") ? "file:" + location : location,
                    StoreUse.CATALOG_ROOT);
            boolean secondOfPlugin = !"hadoop".equals(opened.provider())
                    && (sameConfiguration(opened, dataDirectory)
                    || extras.stream().anyMatch(e -> sameConfiguration(opened, e)));
            if (secondOfPlugin) {
                closeQuietly(opened.store());
                String name = opened.store().configName();
                throw new IllegalArgumentException("Catalog root " + absoluteUri + " needs a second "
                        + opened.provider() + " store" + (name.isEmpty() ? "" : " " + name)
                        + "; this server opens one store per storage plugin configuration: put the"
                        + " catalog's tables below the data directory " + dataDirectory.store().root()
                        + ("s3".equals(opened.provider()) ? " or name a further S3 store for them"
                                + " (arrowflight.s3.stores, arrowflight.s3.<name>.root)" : ""));
            }
            extras.add(opened);
            Located result = holder(location);
            if (result == null) {
                throw new IOException("The store opened for " + absoluteUri + " does not hold it"
                        + " (roots " + opened.store().duckDbRoots() + ")");
            }
            return result;
        }

        /**
         * Tells whether two stores come from the same provider configuration.
         *
         * @param a one store
         * @param b another store
         * @return true for the same provider and configuration name
         */
        private static boolean sameConfiguration(Opened a, Opened b) {
            return a.provider().equals(b.provider())
                    && a.store().configName().equals(b.store().configName());
        }

        /**
         * Finds the open store with the longest root that holds a location.
         *
         * @param absoluteUri location
         * @return the store (built-in stores wrapped to run with the server's class loader) and
         *         the path below its root, or null
         */
        synchronized Located holder(String absoluteUri) {
            String location = CatalogLocations.normalize(absoluteUri);
            Opened best = null;
            String bestPrefix = null;
            List<Opened> all = new ArrayList<>();
            all.add(dataDirectory);
            all.addAll(extras);
            for (Opened opened : all) {
                for (String root : opened.store().duckDbRoots()) {
                    String r = CatalogLocations.normalize(root);
                    String prefix = r.endsWith("/") ? r : r + "/";
                    if ((location.equals(r) || location.startsWith(prefix))
                            && (bestPrefix == null || prefix.length() > bestPrefix.length())) {
                        best = opened;
                        bestPrefix = prefix;
                    }
                }
            }
            if (best == null) {
                return null;
            }
            String relative = location.length() <= bestPrefix.length() ? ""
                    : location.substring(bestPrefix.length());
            DataStore store = best.store() instanceof SpiDataStore ? best.store()
                    : new SpiDataStore(best.store(), opening.server(), best.provider());
            return new Located(store, relative);
        }
    }

    /**
     * Opens one configured catalog from its plugin.
     *
     * @param name catalog provider name
     * @param stores stores handed to the catalog
     * @param opening what opening needs
     * @param localRootsShared whether every node reads the local roots (a shared mount)
     * @param tableSecrets registry the catalog's table credentials go to
     * @return the open catalog
     */
    private static CatalogTables.Entry openCatalog(String name, CatalogStores stores,
            Opening opening, boolean localRootsShared,
            TableSecrets tableSecrets) {
        Map.Entry<PluginManager.LoadedPlugin, CatalogProvider> found = opening.plugins()
                .catalogProvider(name).orElseThrow(() -> new IllegalArgumentException("catalog=" + name
                        + " needs a plugin that provides it (provides.catalog in its descriptor;"
                        + " the iceberg plugin provides iceberg-hadoop and iceberg-rest, the hive"
                        + " plugin hive), but "
                        + (opening.plugins().pluginsDir() == null
                                ? "no plugins directory is configured (arrowflight.plugins.dir or"
                                        + " FLIGHT_PLUGINS_DIR)"
                                : "none in " + opening.plugins().pluginsDir() + " does")));
        ClassLoader loader = found.getKey().loader();
        CatalogProvider provider = found.getValue();
        Catalog catalog;
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            PropertiesPluginConfig pluginConfig = new PropertiesPluginConfig(provider.name(),
                    opening.source(), provider.configKeys());
            pluginConfig.requireNoSecretProperties();
            catalog = provider.open(new DefaultCatalogContext(pluginConfig, stores,
                    opening.redactor(), opening.node(), loader,
                    format -> openFormat(format, stores, opening)));
        } catch (IOException e) {
            throw new UncheckedIOException("Catalog " + name + " cannot be opened: "
                    + opening.redactor().redact(String.valueOf(e.getMessage())), e);
        }
        SpiCatalog wrapped = new SpiCatalog(catalog, loader, provider.name());
        List<String> roots = new ArrayList<>();
        boolean shared = true;
        for (String root : wrapped.readableRoots()) {
            String normalized = CatalogLocations.normalize(root);
            roots.add(normalized);
            String scheme = schemeOf(normalized);
            if ((scheme == null || "file".equals(scheme)) && !localRootsShared) {
                shared = false;
            }
        }
        if (roots.isEmpty()) {
            closeQuietly(wrapped);
            throw new IllegalArgumentException("Catalog " + name + " names no readable roots");
        }
        wrapped.registerSecrets(tableSecrets, name, roots);
        return new CatalogTables.Entry(wrapped, roots, shared);
    }

    /**
     * Opens a new instance of the table format a plugin provides, for a catalog of another plugin.
     *
     * @param name format name
     * @param stores stores handed to the catalog (and to the format)
     * @param opening what opening needs
     * @return the format, its calls wrapped in its plugin's class loader; empty without a plugin
     *         that provides it
     * @throws IOException when the format cannot be opened
     */
    private static Optional<TableFormat> openFormat(String name, CatalogStores stores,
            Opening opening) throws IOException {
        Optional<Map.Entry<PluginManager.LoadedPlugin, TableFormatProvider>> found;
        try {
            found = opening.plugins().formatProvider(name);
        } catch (IllegalStateException e) {
            throw new IOException("Table format " + name + " cannot be loaded: " + e.getMessage(), e);
        }
        if (found.isEmpty()) {
            return Optional.empty();
        }
        ClassLoader loader = found.get().getKey().loader();
        TableFormatProvider provider = found.get().getValue();
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            PropertiesPluginConfig pluginConfig = new PropertiesPluginConfig(provider.name(),
                    opening.source(), provider.configKeys());
            pluginConfig.requireNoSecretProperties();
            TableFormat format = provider.open(new DefaultCatalogContext(pluginConfig, stores,
                    opening.redactor(), opening.node(), loader, null));
            LOGGER.info("node={} catalog=formatOpen format={} plugin={}", opening.node(), name,
                    found.get().getKey().descriptor().name());
            return Optional.of(new SpiTableFormat(format, loader, provider.name()));
        }
    }

    /**
     * Names the schemes of some roots, "file" for plain paths.
     *
     * @param roots roots
     * @return schemes
     */
    private static java.util.Set<String> schemesOf(List<String> roots) {
        java.util.Set<String> result = new java.util.LinkedHashSet<>();
        for (String root : roots) {
            String scheme = schemeOf(root);
            result.add(scheme == null ? "file" : scheme);
        }
        return result;
    }

    /**
     * Closes a catalog, logging a failure.
     *
     * @param catalog catalog
     */
    private static void closeQuietly(SpiCatalog catalog) {
        try {
            catalog.close();
        } catch (IOException | RuntimeException e) {
            LOGGER.warn("node={} catalog=closeFailed provider={} error={}", LogUtil.node(),
                    catalog.provider(), e.toString());
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
        // Compared as the catalogs name them: s3a:// and s3n:// lie under an s3:// root.
        String location = CatalogLocations.normalize(absoluteUri);
        String best = null;
        DataStore bestStore = null;
        List<DataStore> all = new ArrayList<>();
        all.add(dataDirectory);
        all.addAll(extraStores);
        for (DataStore store : all) {
            for (String root : store.duckDbRoots()) {
                String r = CatalogLocations.normalize(root);
                String prefix = r.endsWith("/") ? r : r + "/";
                if ((location.startsWith(prefix) || location.equals(r))
                        && (best == null || prefix.length() > best.length())) {
                    best = prefix;
                    bestStore = store;
                }
            }
        }
        if (best == null) {
            throw new IOException("No open store holds " + absoluteUri);
        }
        String relative = location.length() <= best.length() ? "" : location.substring(best.length());
        return new Located(bestStore, relative);
    }

    @Override
    public void close() {
        catalogTables.close();
        for (DataStore store : extraStores) {
            closeQuietly(store);
        }
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
     * @param use what the store is for
     * @return store as the server uses it
     */
    private static DataStore openSpi(DataStoreProvider provider, ClassLoader loader, String root,
            String storageHost, AppConfig config, SecretRedactor redactor, ConfigSource source,
            String node, StoreUse use) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            PropertiesPluginConfig pluginConfig = new PropertiesPluginConfig(provider.name(), source,
                    provider.configKeys());
            pluginConfig.requireNoSecretProperties();
            DefaultStoreContext context = new DefaultStoreContext(pluginConfig, node, storageHost,
                    config.ioParallelism(), redactor, loader);
            return new SpiDataStore(provider.open(context, root.trim(), use),
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
     * Builds the startup error for a data directory or catalog whose plugin is missing.
     *
     * @param what what needs the plugin, for example "dataDir s3://lake/x"
     * @param plugin plugin name
     * @param plugins plugin manager
     * @return exception to throw
     */
    private static IllegalArgumentException missingPlugin(String what, String plugin,
            PluginManager plugins) {
        String where = plugins.pluginsDir() == null
                ? "no plugins directory is configured (arrowflight.plugins.dir or FLIGHT_PLUGINS_DIR)"
                : "no usable " + plugins.pluginsDir().resolve(plugin) + " directory"
                        + plugins.problem(plugin).map(p -> " (" + p + ")").orElse("")
                        + "; set arrowflight.plugins.dir or FLIGHT_PLUGINS_DIR";
        return new IllegalArgumentException(what + " needs the " + plugin
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
