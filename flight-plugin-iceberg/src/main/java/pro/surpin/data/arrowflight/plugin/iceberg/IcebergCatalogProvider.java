package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens an Iceberg catalog of one kind; the kind's provider name is also its configuration
 * namespace ({@code arrowflight.iceberg-hadoop.warehouse}, {@code FLIGHT_ICEBERG_REST_URI}, ...).
 */
final class IcebergCatalogProvider implements CatalogProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergCatalogProvider.class);

    private final IcebergSettings.Kind kind;

    /**
     * Creates the provider of a kind.
     *
     * @param kind catalog kind
     */
    IcebergCatalogProvider(IcebergSettings.Kind kind) {
        this.kind = kind;
    }

    @Override
    public String name() {
        return kind.providerName();
    }

    @Override
    public List<ConfigKey> configKeys() {
        return IcebergSettings.keys(kind);
    }

    @Override
    public Catalog open(CatalogContext context) throws IOException {
        IcebergSettings settings = IcebergSettings.from(kind, context.config());
        settings.credential().ifPresent(context.redactor()::add);
        settings.token().ifPresent(context.redactor()::add);
        StoresFileIO io = new StoresFileIO(context.stores());
        // Every root is located now, while the server still opens the stores they need.
        for (String root : settings.roots()) {
            context.stores().locate(root);
        }
        TableSource source;
        if (kind == IcebergSettings.Kind.HADOOP) {
            source = new HadoopTableSource(context.stores(), settings.warehouse(), io);
        } else {
            HttpClient http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(settings.requestTimeoutMillis()))
                    .followRedirects(HttpClient.Redirect.NEVER).build();
            source = new RestTableSource(settings, http);
        }
        ExecutorService planPool = Executors.newFixedThreadPool(Math.max(1, settings.planThreads()),
                context.threadFactory(name() + "-plan"));
        LOGGER.info("node={} iceberg=catalogOpen provider={} settings={}", context.nodeName(), name(),
                settings);
        return new IcebergCatalog(name(), settings, source, io, planPool);
    }
}
