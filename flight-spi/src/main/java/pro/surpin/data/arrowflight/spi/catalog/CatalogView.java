package pro.surpin.data.arrowflight.spi.catalog;

import java.util.List;
import java.util.Objects;

/**
 * A view of a catalog: a DuckDB query over tables (or other views) of the same catalog, shown
 * under the server's schema name like a table. The query names every table it reads as
 * {@code "schema"."table"} with the server's schema names, reads nothing else (no files, no
 * table functions), and returns exactly the view's columns, in their order and with their types.
 *
 * @param name view name within its schema
 * @param sql DuckDB {@code SELECT} giving the view's rows
 * @param sources every table or view of this catalog the query reads, in order of first use
 * @param columns the view's columns as the query returns them
 * @param fingerprint changes whenever the query or the columns change
 * @since SPI 1.6
 */
public record CatalogView(String name, String sql, List<TableName> sources,
        List<ColumnSpec> columns, String fingerprint) {

    /**
     * Checks the parts.
     *
     * @param name view name
     * @param sql DuckDB query
     * @param sources tables and views read
     * @param columns columns
     * @param fingerprint fingerprint
     * @throws IllegalArgumentException when the view reads nothing or has no columns
     */
    public CatalogView {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(sql, "sql");
        Objects.requireNonNull(fingerprint, "fingerprint");
        sources = List.copyOf(sources);
        columns = List.copyOf(columns);
        if (sources.isEmpty()) {
            throw new IllegalArgumentException("view " + name + " reads no table");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("view " + name + " has no columns");
        }
    }
}
