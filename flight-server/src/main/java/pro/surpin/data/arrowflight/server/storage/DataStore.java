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
}
