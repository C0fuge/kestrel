package pro.surpin.data.arrowflight.plugin.jdbc;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.CatalogContext;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;
import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens the jdbc catalog ({@code catalog=jdbc}, settings {@code arrowflight.jdbc.*}).
 */
public final class JdbcCatalogProvider implements CatalogProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCatalogProvider.class);

    @Override
    public String name() {
        return "jdbc";
    }

    @Override
    public List<ConfigKey> configKeys() {
        return JdbcSettings.keys();
    }

    @Override
    public Catalog open(CatalogContext context) {
        JdbcSettings settings = JdbcSettings.from(context.config());
        for (JdbcSettings.Source source : settings.sources()) {
            if (source.password() != null) {
                context.redactor().add(source.password());
            }
        }
        LOGGER.info("node={} jdbc=catalogOpen sources={}", context.nodeName(), settings.sources());
        return new JdbcCatalog(settings);
    }
}
