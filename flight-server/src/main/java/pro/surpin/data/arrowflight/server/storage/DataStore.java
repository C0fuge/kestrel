package pro.surpin.data.arrowflight.server.storage;

import org.apache.parquet.io.InputFile;

import java.io.IOException;

import pro.surpin.data.arrowflight.spi.storage.DataFile;

/**
 * A store as the server uses it: the SPI {@link pro.surpin.data.arrowflight.spi.storage.DataStore}
 * plus Parquet footer reads. Stores of plugins are wrapped by {@link SpiDataStore}; built-in
 * stores may read footers their own way.
 */
public interface DataStore extends pro.surpin.data.arrowflight.spi.storage.DataStore {

    /**
     * Opens one file for Parquet footer reads, looking up its length first.
     *
     * @param relativePath file below the data directory
     * @return Parquet input file
     * @throws IOException on storage access failure, including a missing file
     */
    default InputFile inputFile(String relativePath) throws IOException {
        return ParquetInputFiles.of(open(relativePath), relativePath);
    }

    /**
     * Opens a file already listed or looked up, trusting its length.
     *
     * @param file listed file
     * @return Parquet input file
     */
    default InputFile inputFile(DataFile file) {
        return ParquetInputFiles.of(open(file), file.location());
    }

    /**
     * Tells whether DuckDB on a node can read only the files that have every block on that
     * node's own storage host (HDFS DataNodes that require SASL data-transfer protection, read
     * short-circuit only). File hosts and local inventories then name only such holders, and no
     * query may send a file to another node.
     *
     * @return true when only whole-file holders can read a file
     */
    default boolean wholeFileReadsOnly() {
        return false;
    }

    /**
     * Tells whether DuckDB on this node can read a listed file: always, unless only whole-file
     * holders can read files, in which case this node's storage host must hold every block.
     *
     * @param file listed file, with its hosts
     * @return true when this node can read the file
     */
    default boolean readableHere(DataFile file) {
        return true;
    }
}
