package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Objects;

/**
 * A column as a catalog declares it.
 *
 * @param name column name
 * @param duckDbType DuckDB type name, for example {@code DECIMAL(12,2)}
 * @param nullable whether the column may hold nulls
 * @param fieldId Parquet field ID the column is read by (Iceberg), or null to read by name
 */
public record ColumnSpec(String name, String duckDbType, boolean nullable, Integer fieldId) {

    /**
     * Checks the parts.
     *
     * @param name column name
     * @param duckDbType DuckDB type name
     * @param nullable whether nulls are allowed
     * @param fieldId field ID or null
     */
    public ColumnSpec {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(duckDbType, "duckDbType");
    }
}
