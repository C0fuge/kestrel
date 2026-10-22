package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.List;

import pro.surpin.data.arrowflight.spi.Plugin;
import pro.surpin.data.arrowflight.spi.catalog.CatalogProvider;

/**
 * The iceberg plugin: Iceberg tables as table catalogs of the server, from a warehouse directory
 * in the Hadoop layout ({@code catalog=iceberg-hadoop}) or an Iceberg REST catalog
 * ({@code catalog=iceberg-rest}). Metadata is read with Iceberg Java through the server's
 * stores; DuckDB reads the data with its iceberg extension or the snapshot's Parquet files.
 */
public final class IcebergPlugin implements Plugin {

    @Override
    public List<CatalogProvider> catalogProviders() {
        return List.of(new IcebergCatalogProvider(IcebergSettings.Kind.HADOOP),
                new IcebergCatalogProvider(IcebergSettings.Kind.REST));
    }
}
