package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
            return delegate.current(table);
        }
    }

    @Override
    public Optional<TableSnapshot> pinned(TableName table, String snapshotId) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.pinned(table, snapshotId);
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
            return delegate.pinnedColumns(table, snapshotId);
        }
    }

    @Override
    public Optional<TableSnapshot> asOf(TableName table, AsOf point) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.asOf(table, point);
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
