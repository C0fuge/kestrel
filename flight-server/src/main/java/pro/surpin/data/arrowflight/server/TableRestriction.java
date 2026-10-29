package pro.surpin.data.arrowflight.server;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a principal may read of one table: nothing, all of it, or its rows that pass a filter with
 * some columns replaced by masks. Filter and masks are DuckDB SQL over the table's own columns
 * (unqualified names); a mask is cast back to the column's type, so the result schema stays as
 * without restriction.
 *
 * @param allowed whether the table may be read at all
 * @param rowFilter boolean SQL expression the rows read must pass, or null for every row
 * @param masks column name to the SQL expression read in its place, in column order; empty for no
 *        mask
 */
public record TableRestriction(boolean allowed, String rowFilter, Map<String, String> masks) {

    /** The whole table may be read. */
    public static final TableRestriction UNRESTRICTED = new TableRestriction(true, null, Map.of());

    /** Nothing of the table may be read. */
    public static final TableRestriction DENIED = new TableRestriction(false, null, Map.of());

    /**
     * Checks and copies the parts.
     *
     * @param allowed whether the table may be read at all
     * @param rowFilter row filter SQL, or null
     * @param masks column masks
     * @throws IllegalArgumentException when a denial carries a filter or masks, or a filter or mask
     *         is blank
     */
    public TableRestriction {
        masks = masks == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(masks));
        if (!allowed && (rowFilter != null || !masks.isEmpty())) {
            throw new IllegalArgumentException("A denied table has no row filter or masks");
        }
        if (rowFilter != null && rowFilter.isBlank()) {
            throw new IllegalArgumentException("Blank row filter");
        }
        for (Map.Entry<String, String> mask : masks.entrySet()) {
            if (mask.getKey() == null || mask.getKey().isEmpty() || mask.getValue() == null
                    || mask.getValue().isBlank()) {
                throw new IllegalArgumentException("Blank column mask");
            }
        }
    }

    /**
     * Tells whether the table may be read only through a filter or masks.
     *
     * @return true for an allowed table with a row filter or a mask
     */
    public boolean restricts() {
        return allowed && (rowFilter != null || !masks.isEmpty());
    }
}
