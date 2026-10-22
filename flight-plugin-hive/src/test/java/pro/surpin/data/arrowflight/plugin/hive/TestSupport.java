package pro.surpin.data.arrowflight.plugin.hive;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Stream;

import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.config.PluginConfig;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * What the plugin's tests hand a catalog provider instead of the server: local stores, a map
 * configuration and a context.
 */
final class TestSupport {

    private TestSupport() {
    }

    /**
     * Stores that open a local store for every root located before {@link #freeze()}.
     */
    static final class LocalStores implements Stores {
        final List<LocalStore> stores = new ArrayList<>();
        final List<String> located = new ArrayList<>();
        private boolean frozen;

        void freeze() {
            frozen = true;
        }

        @Override
        public synchronized Located locate(String absoluteUri) throws IOException {
            located.add(absoluteUri);
            for (LocalStore store : stores) {
                String prefix = store.root + "/";
                if (absoluteUri.equals(store.root)) {
                    return new Located(store, "");
                }
                if (absoluteUri.startsWith(prefix)) {
                    return new Located(store, absoluteUri.substring(prefix.length()));
                }
            }
            if (frozen || !absoluteUri.startsWith("/")) {
                throw new IOException("No open store holds " + absoluteUri);
            }
            LocalStore store = new LocalStore(absoluteUri);
            stores.add(store);
            return new Located(store, "");
        }
    }

    /**
     * A read-only store over a local directory.
     */
    static final class LocalStore implements DataStore {
        final String root;

        LocalStore(String root) {
            this.root = root;
        }

        private Path path(String relative) {
            return relative.isEmpty() ? Path.of(root) : Path.of(root, relative);
        }

        @Override
        public String kind() {
            return "file";
        }

        @Override
        public boolean shared() {
            return false;
        }

        @Override
        public boolean hasLocality() {
            return false;
        }

        @Override
        public String root() {
            return root;
        }

        @Override
        public boolean exists(String relativePath) {
            return Files.exists(path(relativePath));
        }

        @Override
        public Map<String, String> directories(String relativePath) throws IOException {
            Map<String, String> result = new TreeMap<>();
            try (Stream<Path> list = Files.list(path(relativePath))) {
                for (Path p : list.filter(Files::isDirectory).toList()) {
                    result.put(p.getFileName().toString(), p.toString());
                }
            }
            return result;
        }

        @Override
        public List<DataFile> parquetFiles(String relativeDirectory) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DataFile> files(String relativeDirectory, boolean recursive) throws IOException {
            Path dir = path(relativeDirectory);
            if (!Files.isDirectory(dir)) {
                throw new FileNotFoundException(dir.toString());
            }
            List<DataFile> result = new ArrayList<>();
            try (Stream<Path> walk = recursive ? Files.walk(dir) : Files.list(dir)) {
                for (Path p : walk.filter(Files::isRegularFile).sorted().toList()) {
                    String below = dir.relativize(p).toString();
                    if (!DataStore.hiddenPath(below)) {
                        String relative = Path.of(root).relativize(p).toString();
                        result.add(new DataFile(relative, p.toString(), Files.size(p), 1L, null));
                    }
                }
            }
            return result;
        }

        @Override
        public DataFile file(String relativePath) throws IOException {
            Path p = path(relativePath);
            return new DataFile(relativePath, p.toString(), Files.size(p), 0L, null);
        }

        @Override
        public ReadableFile open(String relativePath) throws IOException {
            Path p = path(relativePath);
            if (!Files.isRegularFile(p)) {
                throw new FileNotFoundException(p.toString());
            }
            long length = Files.size(p);
            return new ReadableFile() {
                @Override
                public long length() {
                    return length;
                }

                @Override
                public void readFully(long position, byte[] buffer, int offset, int count)
                        throws IOException {
                    if (position + count > length) {
                        throw new EOFException();
                    }
                    try (RandomAccessFile f = new RandomAccessFile(p.toFile(), "r")) {
                        f.seek(position);
                        f.readFully(buffer, offset, count);
                    }
                }
            };
        }

        @Override
        public ReadableFile open(DataFile file) {
            try {
                return open(file.relativePath());
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        @Override
        public String duckDbUri(String relativePath) {
            return path(relativePath).toString();
        }

        @Override
        public List<String> duckDbRoots() {
            return List.of(root);
        }

        @Override
        public Map<String, Long> localInventory(String host) {
            return Map.of();
        }
    }

    /**
     * A configuration from a map; secrets come from the same map.
     *
     * @param namespace namespace
     * @param values key to value
     */
    record MapConfig(String namespace, Map<String, String> values) implements PluginConfig {
        @Override
        public Optional<String> get(String key) {
            return Optional.ofNullable(values.get(key)).filter(v -> !v.isBlank());
        }

        @Override
        public Optional<String> secret(String key) {
            return get(key);
        }
    }

    /**
     * A catalog context over local stores.
     *
     * @param config configuration
     * @param stores stores
     * @param redactor redactor
     * @param format iceberg table format, or null
     */
    record Context(PluginConfig config, LocalStores stores, SecretRedactor redactor,
            TableFormat format) implements CatalogContext {

        @Override
        public Optional<TableFormat> tableFormat(String name) {
            return "iceberg".equals(name) ? Optional.ofNullable(format) : Optional.empty();
        }

        @Override
        public String nodeName() {
            return "test";
        }

        @Override
        public ThreadFactory threadFactory(String name) {
            return Executors.defaultThreadFactory();
        }
    }
}
