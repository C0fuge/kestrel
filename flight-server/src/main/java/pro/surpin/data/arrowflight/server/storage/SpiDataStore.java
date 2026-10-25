package pro.surpin.data.arrowflight.server.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import pro.surpin.data.arrowflight.server.plugin.ThreadContext;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;

/**
 * A store of the SPI as the server uses it. Every call runs with the thread context class loader
 * of the store's plugin, so that the plugin's libraries find their own classes.
 */
public final class SpiDataStore implements DataStore {

    private final pro.surpin.data.arrowflight.spi.storage.DataStore delegate;
    private final ClassLoader loader;
    private final String provider;

    /**
     * Wraps a store.
     *
     * @param delegate store of a provider
     * @param loader class loader of the provider's plugin, or null for a built-in provider
     * @param provider provider name, for logs
     */
    public SpiDataStore(pro.surpin.data.arrowflight.spi.storage.DataStore delegate,
            ClassLoader loader, String provider) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.loader = loader;
        this.provider = Objects.requireNonNull(provider, "provider");
    }

    /**
     * Returns the wrapped store.
     *
     * @return store of the provider
     */
    public pro.surpin.data.arrowflight.spi.storage.DataStore delegate() {
        return delegate;
    }

    /**
     * Names the provider of the store.
     *
     * @return provider name
     */
    public String provider() {
        return provider;
    }

    @Override
    public String kind() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.kind();
        }
    }

    @Override
    public boolean shared() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.shared();
        }
    }

    @Override
    public boolean hasLocality() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.hasLocality();
        }
    }

    @Override
    public String root() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.root();
        }
    }

    @Override
    public boolean exists(String relativePath) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.exists(relativePath);
        }
    }

    @Override
    public Map<String, String> directories(String relativePath) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.directories(relativePath);
        }
    }

    @Override
    public List<DataFile> parquetFiles(String relativeDirectory) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.parquetFiles(relativeDirectory);
        }
    }

    @Override
    public List<DataFile> files(String relativeDirectory, boolean recursive) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.files(relativeDirectory, recursive);
        }
    }

    @Override
    public List<DataFile> tableFiles(String schema, String table) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.tableFiles(schema, table);
        }
    }

    @Override
    public DataFile file(String relativePath) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.file(relativePath);
        }
    }

    @Override
    public ReadableFile open(String relativePath) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return wrap(delegate.open(relativePath));
        }
    }

    @Override
    public ReadableFile open(DataFile file) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return wrap(delegate.open(file));
        }
    }

    @Override
    public String duckDbUri(String relativePath) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.duckDbUri(relativePath);
        }
    }

    @Override
    public String duckDbUri(DataFile file) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.duckDbUri(file);
        }
    }

    @Override
    public List<String> duckDbRoots() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return List.copyOf(delegate.duckDbRoots());
        }
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.contributeDuckDb(setup);
        }
    }

    @Override
    public String configName() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.configName();
        }
    }

    @Override
    public Map<String, Long> localInventory(String host) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.localInventory(host);
        }
    }

    @Override
    public String dataVersion() throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.dataVersion();
        }
    }

    @Override
    public String unversionedPath(String relativePath) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.unversionedPath(relativePath);
        }
    }

    @Override
    public ReadScope readScope() throws IOException {
        ReadScope scope;
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            scope = delegate.readScope();
        }
        if (scope == null || scope == ReadScope.NONE) {
            return ReadScope.NONE;
        }
        return () -> {
            try (ThreadContext ignored = ThreadContext.of(loader)) {
                scope.close();
            }
        };
    }

    @Override
    public void close() throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.close();
        }
    }

    @Override
    public String toString() {
        return "SpiDataStore[provider=" + provider + ", root=" + root() + "]";
    }

    /**
     * Makes the reads of a readable file run with the plugin's context class loader too.
     *
     * @param file readable file of the plugin
     * @return readable file
     */
    private ReadableFile wrap(ReadableFile file) {
        if (loader == null) {
            return file;
        }
        return new ReadableFile() {
            @Override
            public long length() {
                try (ThreadContext ignored = ThreadContext.of(loader)) {
                    return file.length();
                }
            }

            @Override
            public void readFully(long position, byte[] buffer, int offset, int length)
                    throws IOException {
                try (ThreadContext ignored = ThreadContext.of(loader)) {
                    file.readFully(position, buffer, offset, length);
                }
            }
        };
    }
}
