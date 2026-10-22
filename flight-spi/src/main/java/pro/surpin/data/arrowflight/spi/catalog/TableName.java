package pro.surpin.data.arrowflight.spi.catalog;

import java.util.Objects;

/**
 * Name of a table in a catalog.
 *
 * @param schema schema (database) name
 * @param table table name
 */
public record TableName(String schema, String table) {

    /**
     * Checks the parts.
     *
     * @param schema schema name
     * @param table table name
     */
    public TableName {
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(table, "table");
    }

    @Override
    public String toString() {
        return schema + "." + table;
    }
}
