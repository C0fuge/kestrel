package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.catalog.TableFormatProvider;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens Iceberg as a table format for catalogs of other plugins (namespace {@code iceberg}:
 * {@code arrowflight.iceberg.planThreads}).
 */
final class IcebergTableFormatProvider implements TableFormatProvider {

    @Override
    public String name() {
        return "iceberg";
    }

    @Override
    public List<ConfigKey> configKeys() {
        return List.of(ConfigKey.of("planThreads", "threads that read manifests", "4"));
    }

    @Override
    public TableFormat open(CatalogContext context) {
        int threads = context.config().getInt("planThreads", 4);
        if (threads < 1) {
            throw new IllegalArgumentException("iceberg.planThreads must be at least 1");
        }
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                context.threadFactory("iceberg-format-plan"));
        return new IcebergTableFormat(new StoresFileIO(context.stores()), pool);
    }
}
