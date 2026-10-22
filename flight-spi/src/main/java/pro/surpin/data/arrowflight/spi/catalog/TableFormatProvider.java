package pro.surpin.data.arrowflight.spi.catalog;

import java.io.IOException;
import java.util.List;

import pro.surpin.data.arrowflight.spi.config.ConfigKey;

/**
 * Opens a table format other catalogs may use, chosen by name (descriptor
 * {@code provides.format}).
 *
 * @since SPI 1.1
 */
public interface TableFormatProvider {

    /**
     * Names the format; also the namespace of its configuration keys.
     *
     * @return name, for example "iceberg"
     */
    String name();

    /**
     * Declares the configuration keys of this format.
     *
     * @return configuration keys
     */
    List<ConfigKey> configKeys();

    /**
     * Opens the format.
     *
     * @param context configuration (namespace = format name), stores, threads
     * @return open format; the server closes it
     * @throws IOException when the format cannot be opened
     */
    TableFormat open(CatalogContext context) throws IOException;
}
