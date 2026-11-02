package pro.surpin.data.arrowflight.server.duckdb;

/**
 * Reads the error DuckDB reports out of the message of a DuckDB JDBC exception.
 */
public final class DuckDbErrors {

    /**
     * First line DuckDB's JDBC driver puts in front of the error of a statement that failed while
     * it ran; the error itself follows on a line starting with {@link #ERROR_LINE}.
     */
    static final String PENDING_WRAPPER =
            "Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result";

    /** Start of the line that carries the wrapped error. */
    static final String ERROR_LINE = "Error: ";

    /** Not instantiable. */
    private DuckDbErrors() {
    }

    /**
     * Returns the line of a DuckDB message that names the error: the first line, or for a
     * statement that failed while it ran the wrapped error after the driver's pending-result line
     * ({@code IO Error: No files found ...} instead of {@code Invalid Input Error: Attempting to
     * execute an unsuccessful or closed pending query result}).
     *
     * @param message exception message, may be null
     * @return the error line, "null" for a null message
     */
    public static String errorLine(String message) {
        String text = String.valueOf(message);
        String first = text.lines().findFirst().orElse(text);
        if (!first.startsWith(PENDING_WRAPPER)) {
            return first;
        }
        return text.lines().skip(1)
                .filter(line -> line.startsWith(ERROR_LINE) && line.length() > ERROR_LINE.length())
                .map(line -> line.substring(ERROR_LINE.length()))
                .findFirst().orElse(first);
    }
}
