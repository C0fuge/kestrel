package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.iceberg.TableMetadata;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;

/**
 * Where an Iceberg catalog finds its namespaces, tables and their current metadata.
 */
interface TableSource extends AutoCloseable {

    /**
     * A table's current metadata.
     *
     * @param metadataLocation location of the metadata file, as written in the catalog
     * @param metadata parsed metadata
     * @param credentials temporary storage credentials the catalog vended, or empty
     */
    record Loaded(String metadataLocation, TableMetadata metadata,
            List<StorageCredential> credentials) {

        /**
         * Copies the credentials.
         *
         * @param metadataLocation metadata file
         * @param metadata parsed metadata
         * @param credentials vended credentials
         */
        public Loaded {
            credentials = credentials == null ? List.of() : List.copyOf(credentials);
        }

        /**
         * Creates a loaded table without vended credentials.
         *
         * @param metadataLocation metadata file
         * @param metadata parsed metadata
         */
        Loaded(String metadataLocation, TableMetadata metadata) {
            this(metadataLocation, metadata, List.of());
        }
    }

    /**
     * Lists the namespaces with one level.
     *
     * @return namespace names
     * @throws IOException when the catalog cannot be read
     */
    List<String> namespaces() throws IOException;

    /**
     * Lists the tables of a namespace.
     *
     * @param namespace namespace name
     * @return table names
     * @throws IOException when the catalog cannot be read
     */
    List<String> tables(String namespace) throws IOException;

    /**
     * Loads a table's current metadata.
     *
     * @param namespace namespace name
     * @param table table name
     * @return metadata, empty when the table does not exist
     * @throws IOException when the catalog cannot be read
     */
    Optional<Loaded> load(String namespace, String table) throws IOException;

    @Override
    default void close() throws IOException {
    }
}
