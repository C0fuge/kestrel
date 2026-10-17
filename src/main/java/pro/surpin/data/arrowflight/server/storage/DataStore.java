package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Read-only access to the directory tree under the data directory: schemas are its top-level
 * directories, tables their subdirectories, and a table's data are the Parquet files below it.
 * All paths are relative to the data directory and use "/" as the separator; the empty path names
 * the data directory itself.
 */
public interface DataStore {

    /**
     * Names the kind of storage, for logs.
     *
     * @return storage kind, for example "hdfs" or "file"
     */
    String kind();

    /**
     * Tells whether every node can read every file (HDFS, object stores), as opposed to a local
     * directory per node, where a node can only read the files it holds itself.
     *
     * @return true unless the data directory is on the local file system
     */
    boolean shared();

    /**
     * Tells whether files have hosts, so that a query can be sent to the node that holds them.
     *
     * @return true when listed files carry the hosts of their blocks
     */
    boolean hasLocality();

    /**
     * Returns the data directory as configured.
     *
     * @return data directory
     */
    String root();

    /**
     * Tells whether a file or directory exists.
     *
     * @param relativePath path below the data directory, empty for the data directory
     * @return true when it exists
     * @throws IOException on storage access failure
     */
    boolean exists(String relativePath) throws IOException;

    /**
     * Lists the directories directly inside a directory.
     *
     * @param relativePath directory below the data directory, empty for the data directory
     * @return directory name to its location, as the store names it
     * @throws IOException on storage access failure, including a missing directory
     */
    Map<String, String> directories(String relativePath) throws IOException;

    /**
     * Lists the Parquet files at any depth below a directory, in path order.
     *
     * @param relativeDirectory directory below the data directory
     * @return Parquet files
     * @throws IOException on storage access failure, including a missing directory
     */
    List<DataFile> parquetFiles(String relativeDirectory) throws IOException;

    /**
     * Lists the Parquet files of a table, in path order.
     *
     * @param schema schema name
     * @param table table name
     * @return Parquet files, empty when the table directory does not exist
     * @throws IOException on storage access failure
     */
    default List<DataFile> tableFiles(String schema, String table) throws IOException {
        String directory = schema + "/" + table;
        return exists(directory) ? parquetFiles(directory) : List.of();
    }

    /**
     * Looks up one file.
     *
     * @param relativePath file below the data directory
     * @return the file, without hosts
     * @throws IOException on storage access failure, including a missing file
     */
    DataFile file(String relativePath) throws IOException;

    /**
     * Opens one file for Parquet footer reads, looking up its length first.
     *
     * @param relativePath file below the data directory
     * @return Parquet input file
     * @throws IOException on storage access failure, including a missing file
     */
    InputFile inputFile(String relativePath) throws IOException;

    /**
     * Opens a file already listed or looked up, trusting its length.
     *
     * @param file listed file
     * @return Parquet input file
     */
    InputFile inputFile(DataFile file);

    /**
     * Names a file the way DuckDB reads it.
     *
     * @param relativePath file below the data directory
     * @return path or URI for read_parquet
     */
    String duckDbUri(String relativePath);

    /**
     * Names a listed file the way DuckDB reads it.
     *
     * @param file listed file
     * @return path or URI for read_parquet
     */
    default String duckDbUri(DataFile file) {
        return duckDbUri(file.relativePath());
    }

    /**
     * Returns the directories DuckDB may read when locked down, as DuckDB sees them.
     *
     * @return readable roots
     */
    List<String> duckDbRoots();

    /**
     * Returns the statements every DuckDB connection runs before it is locked down, so that it
     * can read this store.
     *
     * @return setup statements, empty when DuckDB needs none
     */
    List<String> duckDbSetupStatements();

    /**
     * Lists the Parquet files with at least one block on a storage host.
     *
     * @param host storage host colocated with this server
     * @return relative Parquet path to file size, empty when the data directory does not exist
     * @throws IOException on storage access failure
     */
    Map<String, Long> localInventory(String host) throws IOException;
}
