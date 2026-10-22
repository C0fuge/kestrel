package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import pro.surpin.data.arrowflight.server.plugin.ThreadContext;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * A table format of the SPI as catalogs of other plugins use it: every call runs with the thread
 * context class loader of the format's plugin.
 */
public final class SpiTableFormat implements TableFormat {

    private final TableFormat delegate;
    private final ClassLoader loader;
    private final String name;

    /**
     * Wraps a format.
     *
     * @param delegate format of a provider
     * @param loader class loader of the provider's plugin
     * @param name format name
     */
    public SpiTableFormat(TableFormat delegate, ClassLoader loader, String name) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.loader = loader;
        this.name = Objects.requireNonNull(name, "name");
    }

    /**
     * Names the format.
     *
     * @return format name
     */
    public String name() {
        return name;
    }

    @Override
    public Optional<TableSnapshot> snapshot(TableName table, String metadataLocation,
            String snapshotId) throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.snapshot(table, metadataLocation, snapshotId);
        }
    }

    @Override
    public Optional<String> partialReadProblem(TableSnapshot snapshot) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            return delegate.partialReadProblem(snapshot);
        }
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.contributeDuckDb(setup);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContext ignored = ThreadContext.of(loader)) {
            delegate.close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Cannot close table format " + name, e);
        }
    }
}
