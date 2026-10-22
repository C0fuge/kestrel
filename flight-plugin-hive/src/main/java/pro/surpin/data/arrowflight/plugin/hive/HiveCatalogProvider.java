package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.catalog.TableFormat;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens the Hive Metastore catalog (namespace {@code hive}). The metastore must answer when the
 * server starts: a wrong URI is a startup error, not an empty catalog.
 */
final class HiveCatalogProvider implements CatalogProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveCatalogProvider.class);

    @Override
    public String name() {
        return HiveSettings.NAME;
    }

    @Override
    public List<ConfigKey> configKeys() {
        return HiveSettings.keys();
    }

    @Override
    public Catalog open(CatalogContext context) throws IOException {
        HiveSettings settings = HiveSettings.from(context.config());
        // Every root is located now, while the server still opens the stores they need.
        for (String root : settings.roots()) {
            context.stores().locate(root);
        }
        TableFormat iceberg = null;
        if (settings.icebergTables() != HiveSettings.IcebergTables.SKIP) {
            Optional<TableFormat> format = context.tableFormat("iceberg");
            if (format.isEmpty() && settings.icebergTables() == HiveSettings.IcebergTables.REQUIRE) {
                throw new IllegalArgumentException("hive.icebergTables=require needs the iceberg"
                        + " plugin in the plugins directory");
            }
            iceberg = format.orElse(null);
        }
        Metastore metastore = new ThriftMetastore(settings);
        try {
            List<String> databases = metastore.databases();
            LOGGER.info("node={} hive=catalogOpen settings={} databases={} icebergFormat={}",
                    context.nodeName(), settings, databases.size(), iceberg != null);
        } catch (IOException | RuntimeException e) {
            metastore.close();
            if (iceberg != null) {
                closeQuietly(iceberg);
            }
            throw e;
        }
        ExecutorService listPool = Executors.newFixedThreadPool(settings.listThreads(),
                context.threadFactory("hive-list"));
        return new HiveCatalog(settings, metastore, context.stores(), iceberg, listPool,
                context.nodeName());
    }

    /**
     * Closes a format after a failed open.
     *
     * @param format format
     */
    private static void closeQuietly(TableFormat format) {
        try {
            format.close();
        } catch (Exception e) {
            LOGGER.debug("hive=formatCloseFailed error={}", e.toString());
        }
    }
}
