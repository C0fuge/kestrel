package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Objects;

/**
 * A column as a catalog declares it.
 *
 * @param name column name
 * @param duckDbType DuckDB type name, for example {@code DECIMAL(12,2)}
 * @param nullable whether the column may hold nulls
 * @param fieldId Parquet field ID the column is read by (Iceberg), or null to read by name
 * @param sourceType the type as the catalog itself declares it (a Hive type such as {@code char(10)}), or null
 *        when the catalog gives none (since SPI 1.8)
 */
public record ColumnSpec(String name, String duckDbType, boolean nullable, Integer fieldId, String sourceType) {

    /**
     * Checks the parts.
     *
     * @param name column name
     * @param duckDbType DuckDB type name
     * @param nullable whether nulls are allowed
     * @param fieldId field ID or null
     * @param sourceType catalog type or null
     */
    public ColumnSpec {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(duckDbType, "duckDbType");
    }

    /**
     * Creates a column without a catalog type (the constructor of SPI 1.0 to 1.7).
     *
     * @param name column name
     * @param duckDbType DuckDB type name
     * @param nullable whether nulls are allowed
     * @param fieldId field ID or null
     */
    public ColumnSpec(String name, String duckDbType, boolean nullable, Integer fieldId) {
        this(name, duckDbType, nullable, fieldId, null);
    }
}
