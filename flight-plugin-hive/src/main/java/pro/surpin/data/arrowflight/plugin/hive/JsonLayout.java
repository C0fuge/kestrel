package pro.surpin.data.arrowflight.plugin.hive;

/**
 * Which of Hive 3.1.3's two JSON serdes reads a text table, one JSON object per line
 * ({@link JsonRows}). They parse alike and differ only in how a key that names no column is
 * matched to Hive's internal column names {@code _col<n>}.
 *
 * @param hcatalog true for {@code org.apache.hive.hcatalog.data.JsonSerDe}, false for
 *     {@code org.apache.hadoop.hive.serde2.JsonSerDe}
 */
record JsonLayout(boolean hcatalog) {

    /** HCatalog's serde, the usual one of Hive 3.1 JSON tables. */
    static final String HCATALOG_SERDE = "org.apache.hive.hcatalog.data.JsonSerDe";
    /** The serde2 copy of it. */
    static final String SERDE2_SERDE = "org.apache.hadoop.hive.serde2.JsonSerDe";

    /**
     * Tells which JSON serde a serialization library names.
     *
     * @param serializationLib a storage descriptor's serde class, or null
     * @return the layout, or null for any other serde (other JSON serdes such as OpenX's parse
     *     differently and are not read)
     */
    static JsonLayout of(String serializationLib) {
        if (HCATALOG_SERDE.equals(serializationLib)) {
            return new JsonLayout(true);
        }
        return SERDE2_SERDE.equals(serializationLib) ? new JsonLayout(false) : null;
    }

    /**
     * Describes the layout for the table's schema ID.
     *
     * @return {@code h} or {@code s}
     */
    String canonical() {
        return hcatalog ? "h" : "s";
    }
}
