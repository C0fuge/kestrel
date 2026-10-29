package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import pro.surpin.data.arrowflight.server.duckdb.TableSecrets;

import pro.surpin.data.arrowflight.server.plugin.ThreadContext;
import pro.surpin.data.arrowflight.spi.catalog.ArrowScan;
import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.AsOf;
import pro.surpin.data.arrowflight.spi.catalog.CatalogView;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * A catalog of the SPI as the server uses it: every call runs with the thread context class
 * loader of the catalog's plugin.
 */
public final class SpiCatalog implements Catalog {

    private final Catalog delegate;
    private final ClassLoader loader;
    private final String provider;
    /** Registry of the tables' storage credentials, or null until {@link #registerSecrets}. */
    private volatile TableSecrets secrets;
    /** Catalog name for the secrets. */
    private volatile String catalogName;
    /** The catalog's readable roots, normalised, for the secrets. */
    private volatile List<String> secretRoots = List.of();

    /**
     * Wraps a catalog.
     *
     * @param delegate catalog of a provider
     * @param loader class loader of the provider's plugin, or null for a built-in provider
     * @param provider provider name
     */
    public SpiCatalog(Catalog delegate, ClassLoader loader, String provider) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.loader = loader;
        this.provider = Objects.requireNonNull(provider, "provider");
    }

    /**
     * Registers the storage credentials of every snapshot this catalog returns from now on, so
     * that DuckDB connections install them before they read the table's files.
     *
     * @param registry registry of table secrets
     * @param name catalog name
     * @param roots the catalog's readable roots, normalised
     */
    public void registerSecrets(TableSecrets registry, String name, List<String> roots) {
        this.secretRoots = List.copyOf(roots);
        this.catalogName = Objects.requireNonNull(name, "name");
        this.secrets = Objects.requireNonNull(registry, "registry");
    }

    /**
     * Registers the credentials of a returned snapshot.
     *
     * @param snapshot snapshot, or empty
     * @return the snapshot
     */
    private Optional<TableSnapshot> registered(Optional<TableSnapshot> snapshot) {
        TableSecrets registry = secrets;
        if (registry != null && snapshot.isPresent()) {
            registry.register(catalogName, secretRoots, snapshot.get());
        }
        return snapshot;
    }

    /**
     * Names the provider of the catalog.
     *
     * @return provider name
     */
    public String provider() {
        return provider;
    }

    @Override
    public List<String> schemas() throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return List.copyOf(delegate.schemas());
        }
    }

    @Override
    public List<String> tables(String schema) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return List.copyOf(delegate.tables(schema));
        }
    }

    @Override
    public List<CatalogView> views(String schema) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return List.copyOf(delegate.views(schema));
        }
    }

    @Override
    public Optional<TableSnapshot> current(TableName table) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return registered(delegate.current(table));
        }
    }

    @Override
    public Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return registered(delegate.pinned(table, snapshotId));
        }
    }

    @Override
    public void exportArrowScan(TableSnapshot snapshot, List<TableFile> files, ArrowScan scan,
            long streamAddress, Object allocator) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.exportArrowScan(snapshot, files, scan, streamAddress, allocator);
        }
    }

    @Override
    public Optional<TableSnapshot> pinnedColumns(TableName table, String snapshotId)
            throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return registered(delegate.pinnedColumns(table, snapshotId));
        }
    }

    @Override
    public Optional<TableSnapshot> asOf(TableName table, AsOf point) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return registered(delegate.asOf(table, point));
        }
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.partialReadProblem(snapshot);
        }
    }

    @Override
    public List<String> readableRoots() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return List.copyOf(delegate.readableRoots());
        }
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.contributeDuckDb(setup);
        }
    }

    @Override
    public boolean namesAreStatic() {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.namesAreStatic();
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot close catalog " + provider, e);
        }
    }
}
