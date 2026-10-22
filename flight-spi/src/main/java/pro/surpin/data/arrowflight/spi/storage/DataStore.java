package pro.surpin.data.arrowflight.spi.storage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;

/**
 * Read-only access to the directory tree under a root: in the data directory, schemas are its
 * top-level directories, tables their subdirectories, and a table's data are the Parquet files
 * below it. All paths are relative to the root and use "/" as the separator; the empty path
 * names the root itself.
 */
public interface DataStore extends AutoCloseable {

    /**
     * Names the kind of storage, for logs.
     *
     * @return storage kind, for example "hdfs", "file" or "s3"
     */
    String kind();

    /**
     * Tells whether every node can read every file (HDFS, object stores), as opposed to a local
     * directory per node, where a node can only read the files it holds itself.
     *
     * @return true unless the root is on a node's local file system
     */
    boolean shared();

    /**
     * Tells whether files have hosts, so that a query can be sent to the node that holds them.
     *
     * @return true when listed files carry the hosts of their blocks
     */
    boolean hasLocality();

    /**
     * Returns the root as configured.
     *
     * @return root
     */
    String root();

    /**
     * Tells whether a file or directory exists.
     *
     * @param relativePath path below the root, empty for the root
     * @return true when it exists
     * @throws IOException on storage access failure
     */
    boolean exists(String relativePath) throws IOException;

    /**
     * Lists the directories directly inside a directory.
     *
     * @param relativePath directory below the root, empty for the root
     * @return directory name to its location, as the store names it
     * @throws IOException on storage access failure, including a missing directory
     */
    Map<String, String> directories(String relativePath) throws IOException;

    /**
     * Lists the Parquet files at any depth below a directory, in path order.
     *
     * @param relativeDirectory directory below the root
     * @return Parquet files
     * @throws IOException on storage access failure, including a missing directory
     */
    List<DataFile> parquetFiles(String relativeDirectory) throws IOException;

    /**
     * Lists the data files of a directory whatever their names (Hive and Trino write files
     * without the ".parquet" extension), skipping hidden names that start with "_" or "." and
     * everything below such directories; in path order. The default lists the Parquet files.
     *
     * @param relativeDirectory directory below the root
     * @param recursive whether files of subdirectories count too
     * @return files
     * @throws IOException on storage access failure, including a missing directory
     * @since SPI 1.1
     */
    default List<DataFile> files(String relativeDirectory, boolean recursive) throws IOException {
        String prefix = relativeDirectory.isEmpty() || relativeDirectory.endsWith("/")
                ? relativeDirectory : relativeDirectory + "/";
        List<DataFile> result = new java.util.ArrayList<>();
        for (DataFile file : parquetFiles(relativeDirectory)) {
            String below = file.relativePath().startsWith(prefix)
                    ? file.relativePath().substring(prefix.length()) : file.relativePath();
            if (!hiddenPath(below) && (recursive || below.indexOf('/') < 0)) {
                result.add(file);
            }
        }
        return result;
    }

    /**
     * Tells whether a relative path has a hidden segment (a name starting with "_" or ".").
     *
     * @param relativePath path below a listed directory
     * @return true when a segment is hidden
     * @since SPI 1.1
     */
    static boolean hiddenPath(String relativePath) {
        for (String segment : relativePath.split("/")) {
            if (segment.startsWith("_") || segment.startsWith(".")) {
                return true;
            }
        }
        return false;
    }

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
     * @param relativePath file below the root
     * @return the file, without hosts
     * @throws IOException on storage access failure, including a missing file
     */
    DataFile file(String relativePath) throws IOException;

    /**
     * Opens one file for positional reads, looking up its length first.
     *
     * @param relativePath file below the root
     * @return readable file
     * @throws IOException on storage access failure, including a missing file
     */
    ReadableFile open(String relativePath) throws IOException;

    /**
     * Opens a file already listed or looked up, trusting its length.
     *
     * @param file listed file
     * @return readable file
     */
    ReadableFile open(DataFile file);

    /**
     * Names a file the way DuckDB reads it.
     *
     * @param relativePath file below the root
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
     * Returns the directories DuckDB may read when locked down, as DuckDB sees them. The server
     * checks each one and builds DuckDB's allowed directories from them.
     *
     * @return readable roots
     */
    List<String> duckDbRoots();

    /**
     * Describes what every DuckDB connection needs before it is locked down so that it can read
     * this store: extensions, settings and secrets. The server checks each item against its
     * allowlists and writes the statements itself.
     *
     * @param setup receives the items
     */
    default void contributeDuckDb(DuckDbSetup setup) {
    }

    /**
     * Lists the Parquet files with at least one block on a storage host.
     *
     * @param host storage host colocated with this server
     * @return relative Parquet path to file size, empty when the root does not exist or files
     *         have no hosts
     * @throws IOException on storage access failure
     */
    Map<String, Long> localInventory(String host) throws IOException;

    /**
     * Releases clients and threads of the store.
     *
     * @throws IOException when releasing fails
     */
    @Override
    default void close() throws IOException {
    }
}
