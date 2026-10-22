package pro.surpin.data.arrowflight.server.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.SpiVersion;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.catalog.TableFormatProvider;
import pro.surpin.data.arrowflight.spi.storage.DataStoreProvider;

/**
 * Finds plugins in {@code <pluginsDir>/<name>/*.jar} and loads only those the configuration
 * needs. Each plugin's descriptor is read from its jars first, without loading a class; a plugin
 * is loaded only when its version matches this server's SPI, its jars carry nothing that must
 * exist once per JVM (DuckDB, Arrow memory, an slf4j binding, the SPI itself, a Hadoop
 * FileSystem service) and, when there is a {@code SHA256SUMS} file or checksums are required,
 * every jar matches it. Then exactly one {@link Plugin} is found with {@link ServiceLoader} in
 * the plugin's own {@link PluginClassLoader}.
 */
public final class PluginManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);
    /** Jar entries a plugin may not carry: the server's copy must be the only one in the JVM. */
    private static final List<String> REFUSED_PREFIXES = List.of(
            "org/duckdb/", "org/apache/arrow/memory/", "org/slf4j/impl/", "org/slf4j/simple/",
            "pro/surpin/data/arrowflight/spi/");
    /** Service file of Hadoop file systems, which Hadoop loads once per JVM. */
    private static final String HADOOP_FILE_SYSTEMS = "META-INF/services/org.apache.hadoop.fs.FileSystem";
    /** Checksums of a plugin directory, as written by sha256sum. */
    public static final String CHECKSUMS = "SHA256SUMS";

    private final Path pluginsDir;
    private final boolean requireChecksums;
    private final ClassLoader server;
    private final String node;
    private final Map<String, PluginDescriptor> present = new LinkedHashMap<>();
    private final Map<String, LoadedPlugin> loaded = new LinkedHashMap<>();
    private final Map<String, String> invalid = new LinkedHashMap<>();

    /**
     * A loaded plugin.
     *
     * @param descriptor its descriptor
     * @param loader its class loader
     * @param plugin its entry point
     * @param storage its storage providers
     * @param catalogs its catalog providers
     * @param formats its table format providers
     */
    public record LoadedPlugin(PluginDescriptor descriptor, PluginClassLoader loader, Plugin plugin,
            List<DataStoreProvider> storage, List<CatalogProvider> catalogs,
            List<TableFormatProvider> formats) {
    }

    /**
     * Scans a plugins directory; loads nothing yet.
     *
     * @param pluginsDir plugins directory, or null when there is none
     * @param requireChecksums whether every plugin directory needs a matching SHA256SUMS
     * @param server the server's class loader
     * @param node this node, for logs
     */
    public PluginManager(Path pluginsDir, boolean requireChecksums, ClassLoader server, String node) {
        this.pluginsDir = pluginsDir;
        this.requireChecksums = requireChecksums;
        this.server = Objects.requireNonNull(server, "server");
        this.node = node;
        if (pluginsDir == null || !Files.isDirectory(pluginsDir)) {
            LOGGER.info("node={} plugins=none dir={}", node, pluginsDir);
            return;
        }
        try (Stream<Path> dirs = Files.list(pluginsDir)) {
            for (Path dir : dirs.filter(Files::isDirectory).sorted().toList()) {
                Path fileName = dir.getFileName();
                String name = fileName == null ? dir.toString() : fileName.toString();
                try {
                    PluginDescriptor descriptor = PluginDescriptor.read(dir);
                    present.put(descriptor.name(), descriptor);
                } catch (IllegalArgumentException | UncheckedIOException e) {
                    // Only an error when the configuration needs this plugin.
                    invalid.put(name, e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot list plugins directory " + pluginsDir, e);
        }
    }

    /**
     * Returns the plugins directory.
     *
     * @return directory, or null
     */
    public Path pluginsDir() {
        return pluginsDir;
    }

    /**
     * Returns the plugins found, loaded or not.
     *
     * @return plugin name to descriptor
     */
    public Map<String, PluginDescriptor> present() {
        return Map.copyOf(present);
    }

    /**
     * Returns the plugins loaded so far.
     *
     * @return plugin name to loaded plugin
     */
    public Map<String, LoadedPlugin> loaded() {
        return Map.copyOf(loaded);
    }

    /**
     * Finds the plugin whose descriptor claims a storage scheme, and loads it.
     *
     * @param scheme URI scheme, lower case
     * @return the plugin and its provider for the scheme, empty when no plugin claims it
     * @throws IllegalStateException when the plugin cannot be loaded or does not provide the
     *         scheme it claims
     */
    public synchronized Optional<Map.Entry<LoadedPlugin, DataStoreProvider>> storageProvider(
            String scheme) {
        PluginDescriptor claiming = null;
        for (PluginDescriptor descriptor : present.values()) {
            if (descriptor.storageSchemes().contains(scheme)) {
                if (claiming != null) {
                    throw new IllegalStateException("Plugins " + claiming.name() + " and "
                            + descriptor.name() + " both claim the storage scheme " + scheme);
                }
                claiming = descriptor;
            }
        }
        if (claiming == null) {
            return Optional.empty();
        }
        LoadedPlugin plugin = load(claiming);
        for (DataStoreProvider provider : plugin.storage()) {
            if (provider.schemes().stream().anyMatch(s -> s.equalsIgnoreCase(scheme))) {
                return Optional.of(Map.entry(plugin, provider));
            }
        }
        throw new IllegalStateException("Plugin " + claiming.name() + " claims the storage scheme "
                + scheme + " in its descriptor but has no provider for it");
    }

    /**
     * Finds the plugin whose descriptor provides a table catalog and loads it.
     *
     * @param name catalog provider name, for example "iceberg-hadoop"
     * @return the loaded plugin and its provider, empty when no plugin claims the name
     * @throws IllegalStateException when two plugins claim it, or the plugin cannot be loaded or
     *         has no provider of that name
     */
    public synchronized Optional<Map.Entry<LoadedPlugin, CatalogProvider>> catalogProvider(
            String name) {
        PluginDescriptor claiming = null;
        for (PluginDescriptor descriptor : present.values()) {
            if (descriptor.catalogs().contains(name)) {
                if (claiming != null) {
                    throw new IllegalStateException("Plugins " + claiming.name() + " and "
                            + descriptor.name() + " both provide the catalog " + name);
                }
                claiming = descriptor;
            }
        }
        if (claiming == null) {
            return Optional.empty();
        }
        LoadedPlugin plugin = load(claiming);
        for (CatalogProvider provider : plugin.catalogs()) {
            if (provider.name().equalsIgnoreCase(name)) {
                return Optional.of(Map.entry(plugin, provider));
            }
        }
        throw new IllegalStateException("Plugin " + claiming.name() + " names the catalog " + name
                + " in its descriptor but has no provider for it");
    }

    /**
     * Finds the plugin whose descriptor provides a table format and loads it.
     *
     * @param name format name, for example "iceberg"
     * @return the loaded plugin and its provider, empty when no plugin claims the name
     * @throws IllegalStateException when two plugins claim it, or the plugin cannot be loaded or
     *         has no provider of that name
     */
    public synchronized Optional<Map.Entry<LoadedPlugin, TableFormatProvider>> formatProvider(
            String name) {
        PluginDescriptor claiming = null;
        for (PluginDescriptor descriptor : present.values()) {
            if (descriptor.formats().contains(name)) {
                if (claiming != null) {
                    throw new IllegalStateException("Plugins " + claiming.name() + " and "
                            + descriptor.name() + " both provide the table format " + name);
                }
                claiming = descriptor;
            }
        }
        if (claiming == null) {
            return Optional.empty();
        }
        LoadedPlugin plugin = load(claiming);
        for (TableFormatProvider provider : plugin.formats()) {
            if (provider.name().equalsIgnoreCase(name)) {
                return Optional.of(Map.entry(plugin, provider));
            }
        }
        throw new IllegalStateException("Plugin " + claiming.name() + " names the table format "
                + name + " in its descriptor but has no provider for it");
    }

    /**
     * Describes why a plugin directory could not be read, for error messages.
     *
     * @param name plugin directory name
     * @return problem, empty when the directory is fine or absent
     */
    public Optional<String> problem(String name) {
        return Optional.ofNullable(invalid.get(name));
    }

    /**
     * Logs the plugins that were found but not needed.
     */
    public void logUnused() {
        for (PluginDescriptor descriptor : present.values()) {
            if (!loaded.containsKey(descriptor.name())) {
                LOGGER.info("node={} plugin=present name={} enabled=false dir={}", node,
                        descriptor.name(), descriptor.directory());
            }
        }
        invalid.forEach((name, problem) -> LOGGER.warn(
                "node={} plugin=unreadable name={} enabled=false problem={}", node, name, problem));
    }

    /**
     * Loads a plugin once.
     *
     * @param descriptor its descriptor
     * @return loaded plugin
     */
    private LoadedPlugin load(PluginDescriptor descriptor) {
        LoadedPlugin existing = loaded.get(descriptor.name());
        if (existing != null) {
            return existing;
        }
        String name = descriptor.name();
        boolean supported;
        try {
            supported = SpiVersion.supports(descriptor.spi());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Plugin " + name + ": " + e.getMessage(), e);
        }
        if (!supported) {
            throw new IllegalStateException("Plugin " + name + " needs SPI " + descriptor.spi()
                    + ", server has " + SpiVersion.current());
        }
        checkJars(descriptor);
        checkChecksums(descriptor);
        List<URL> urls = new ArrayList<>();
        for (Path jar : descriptor.jars()) {
            try {
                urls.add(jar.toUri().toURL());
            } catch (MalformedURLException e) {
                throw new IllegalStateException("Plugin " + name + ": bad jar path " + jar, e);
            }
        }
        PluginClassLoader loader = new PluginClassLoader(name, urls.toArray(new URL[0]), server,
                descriptor.shareHadoop());
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            List<Plugin> plugins = new ArrayList<>();
            for (Plugin plugin : ServiceLoader.load(Plugin.class, loader)) {
                plugins.add(plugin);
            }
            if (plugins.size() != 1) {
                throw new IllegalStateException("Plugin " + name + " must have exactly one "
                        + Plugin.class.getName() + " service, has " + plugins.size());
            }
            Plugin plugin = plugins.get(0);
            List<DataStoreProvider> storage = List.copyOf(plugin.dataStoreProviders());
            List<CatalogProvider> catalogs = List.copyOf(plugin.catalogProviders());
            List<TableFormatProvider> formats = List.copyOf(plugin.tableFormatProviders());
            for (DataStoreProvider provider : storage) {
                for (String scheme : provider.schemes()) {
                    for (LoadedPlugin other : loaded.values()) {
                        if (other.storage().stream().anyMatch(p -> p.schemes().contains(scheme))) {
                            throw new IllegalStateException("Storage scheme " + scheme
                                    + " is provided by both " + other.descriptor().name() + " and "
                                    + name);
                        }
                    }
                }
            }
            LoadedPlugin result = new LoadedPlugin(descriptor, loader, plugin, storage, catalogs,
                    formats);
            loaded.put(name, result);
            LOGGER.info("node={} plugin=loaded name={} spi={} jars={} storage={} catalogs={}"
                    + " formats={} shareHadoop={} dir={}", node, name, descriptor.spi(),
                    descriptor.jars().size(), storage.stream().map(DataStoreProvider::name).toList(),
                    catalogs.stream().map(CatalogProvider::name).toList(),
                    formats.stream().map(TableFormatProvider::name).toList(),
                    descriptor.shareHadoop(), descriptor.directory());
            return result;
        } catch (RuntimeException | ServiceConfigurationError | LinkageError e) {
            closeQuietly(loader);
            throw e instanceof IllegalStateException ise ? ise
                    : new IllegalStateException("Plugin " + name + " cannot be loaded: " + e, e);
        }
    }

    /**
     * Refuses jars that carry what must exist once per JVM.
     *
     * @param descriptor plugin
     */
    static void checkJars(PluginDescriptor descriptor) {
        for (Path jar : descriptor.jars()) {
            Path fileName = jar.getFileName();
            String jarName = fileName == null ? "" : fileName.toString().toLowerCase(Locale.ROOT);
            if (descriptor.shareHadoop() && jarName.startsWith("hadoop-common")) {
                throw new IllegalStateException("Plugin " + descriptor.name() + " shares Hadoop but"
                        + " ships " + jarName + ": the server's Hadoop must be the only one");
            }
            try (ZipFile zip = new ZipFile(jar.toFile())) {
                Enumeration<? extends ZipEntry> entries = zip.entries();
                while (entries.hasMoreElements()) {
                    String entry = entries.nextElement().getName();
                    for (String refused : REFUSED_PREFIXES) {
                        if (entry.startsWith(refused)) {
                            throw new IllegalStateException("Plugin " + descriptor.name()
                                    + " is refused: " + jarName + " contains " + refused
                                    + " (the server's copy must be the only one in the JVM)");
                        }
                    }
                    if (HADOOP_FILE_SYSTEMS.equals(entry)) {
                        throw new IllegalStateException("Plugin " + descriptor.name()
                                + " is refused: " + jarName + " registers Hadoop file systems ("
                                + HADOOP_FILE_SYSTEMS + "); plugins read files through the server's"
                                + " stores");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read plugin jar " + jar, e);
            }
        }
    }

    /**
     * Checks the jars against SHA256SUMS: always when the file exists, and its absence is an
     * error when checksums are required. Every jar must be listed and match.
     *
     * @param descriptor plugin
     */
    private void checkChecksums(PluginDescriptor descriptor) {
        Path sums = descriptor.directory().resolve(CHECKSUMS);
        if (!Files.isRegularFile(sums)) {
            if (requireChecksums) {
                throw new IllegalStateException("Plugin " + descriptor.name() + " has no "
                        + CHECKSUMS + " and arrowflight.plugins.requireChecksums=true");
            }
            return;
        }
        Map<String, String> expected = new LinkedHashMap<>();
        try {
            for (String line : Files.readAllLines(sums, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                String[] parts = trimmed.split("\\s+\\*?", 2);
                if (parts.length != 2) {
                    throw new IllegalStateException("Plugin " + descriptor.name() + ": bad line in "
                            + CHECKSUMS + ": " + trimmed);
                }
                String file = parts[1].startsWith("./") ? parts[1].substring(2) : parts[1];
                expected.put(file, parts[0].toLowerCase(Locale.ROOT));
            }
            for (Path jar : descriptor.jars()) {
                Path fileName = jar.getFileName();
                String jarName = fileName == null ? "" : fileName.toString();
                String want = expected.get(jarName);
                if (want == null) {
                    throw new IllegalStateException("Plugin " + descriptor.name() + ": " + jarName
                            + " is not listed in " + CHECKSUMS);
                }
                if (!want.equals(sha256(jar))) {
                    throw new IllegalStateException("Plugin " + descriptor.name() + ": " + jarName
                            + " does not match its checksum in " + CHECKSUMS);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot check " + sums, e);
        }
    }

    /**
     * Computes the SHA-256 of a file.
     *
     * @param file file
     * @return lower case hex digest
     * @throws IOException when the file cannot be read
     */
    static String sha256(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        byte[] buffer = new byte[64 * 1024];
        try (InputStream input = Files.newInputStream(file)) {
            int n;
            while ((n = input.read(buffer)) > 0) {
                digest.update(buffer, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    @Override
    public synchronized void close() {
        for (LoadedPlugin plugin : loaded.values()) {
            closeQuietly(plugin.loader());
        }
        loaded.clear();
    }

    /**
     * Closes a loader, logging a failure.
     *
     * @param loader loader
     */
    private void closeQuietly(PluginClassLoader loader) {
        try {
            loader.close();
        } catch (IOException e) {
            LOGGER.warn("node={} plugin=closeFailed loader={} error={}", node, loader.getName(),
                    e.toString());
        }
    }
}
