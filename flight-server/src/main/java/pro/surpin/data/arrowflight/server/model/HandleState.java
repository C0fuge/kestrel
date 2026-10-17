package pro.surpin.data.arrowflight.server.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * State associated with a query handle, including execution and load-accounting data.
 *
 * @param query SQL query text
 * @param filePaths paths to Parquet files
 * @param serverUri URI of the server holding the files
 * @param bytes total file size
 * @param loadTracked whether the bytes were added to distributed server load
 * @param complete whether the whole query runs on the receiving node and returns the complete
 *        answer, instead of a part computed from the listed files
 * @param parameters values of the query's parameters as a one-row Arrow IPC stream, or null
 */
public record HandleState(String query, String[] filePaths, String serverUri,
        long bytes, boolean loadTracked, boolean complete, byte[] parameters) implements Serializable {

    /**
     * Creates state without parameter values.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @param loadTracked whether the bytes were added to distributed server load
     * @param complete whether the receiving node returns the complete answer
     */
    public HandleState(String query, String[] filePaths, String serverUri,
            long bytes, boolean loadTracked, boolean complete) {
        this(query, filePaths, serverUri, bytes, loadTracked, complete, null);
    }

    /**
     * Creates state for a part of a query computed from the listed files.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @param loadTracked whether the bytes were added to distributed server load
     */
    public HandleState(String query, String[] filePaths, String serverUri,
            long bytes, boolean loadTracked) {
        this(query, filePaths, serverUri, bytes, loadTracked, false);
    }

    /**
     * Creates state for a query that returns its complete answer from one node.
     *
     * @param query SQL query text
     * @return endpoint state
     */
    public static HandleState forCompleteAnswer(String query) {
        return new HandleState(query, null, null, 0L, false, true, null);
    }

    /**
     * Creates state for a query with parameter values that returns its complete answer from one node.
     *
     * @param query SQL query text
     * @param parameters one-row Arrow IPC stream with the values, or null
     * @return endpoint state
     */
    public static HandleState forCompleteAnswer(String query, byte[] parameters) {
        return new HandleState(query, null, null, 0L, false, true, parameters);
    }

    /**
     * Compares handle states using file-path contents.
     *
     * @param other object to compare
     * @return whether all state values are equal
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof HandleState state
                && bytes == state.bytes
                && loadTracked == state.loadTracked
                && complete == state.complete
                && Arrays.equals(parameters, state.parameters)
                && Objects.equals(query, state.query)
                && Arrays.equals(filePaths, state.filePaths)
                && Objects.equals(serverUri, state.serverUri);
    }

    /**
     * Computes a hash code using file-path contents.
     *
     * @return content-based hash code
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(query, serverUri, bytes, loadTracked, complete);
        return 31 * (31 * result + Arrays.hashCode(filePaths)) + Arrays.hashCode(parameters);
    }

    /**
     * Formats handle state using file-path contents.
     *
     * @return content-based state representation
     */
    @Override
    public String toString() {
        return "HandleState[query=" + query
                + ", filePaths=" + Arrays.toString(filePaths)
                + ", serverUri=" + serverUri
                + ", bytes=" + bytes
                + ", loadTracked=" + loadTracked
                + ", complete=" + complete
                + ", parameterBytes=" + (parameters == null ? 0 : parameters.length) + "]";
    }

    /**
     * Creates state compatible with the original four-field representation.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     */
    public HandleState(String query, String[] filePaths, String serverUri, long bytes) {
        this(query, filePaths, serverUri, bytes, serverUri != null && bytes > 0);
    }

    /**
     * @param query SQL query text
     * @return handle state with no file paths or server URI
     */
    public static HandleState forQuery(String query) {
        return new HandleState(query, null, null, 0L, false);
    }

    /**
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @return handle state with all fields populated
     */
    public static HandleState forServerFiles(String query, String[] filePaths, String serverUri, long bytes) {
        return new HandleState(query, filePaths, serverUri, bytes, true);
    }

    /**
     * Creates endpoint state with explicit distributed load-accounting behavior.
     *
     * @param query SQL query text
     * @param filePaths paths to Parquet files
     * @param serverUri URI of the server holding the files
     * @param bytes total file size
     * @param loadTracked whether the bytes were added to distributed server load
     * @return handle state with all fields populated
     */
    public static HandleState forServerFiles(String query, String[] filePaths,
            String serverUri, long bytes, boolean loadTracked) {
        return new HandleState(query, filePaths, serverUri, bytes, loadTracked);
    }
}
