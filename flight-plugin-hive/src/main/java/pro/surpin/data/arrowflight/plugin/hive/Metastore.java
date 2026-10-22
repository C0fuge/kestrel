package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * The read calls of a Hive Metastore this plugin makes. A metastore that cannot be reached is an
 * {@link IOException} naming it, never an empty answer.
 */
interface Metastore extends AutoCloseable {

    /**
     * Lists the databases.
     *
     * @return database names
     * @throws IOException when the metastore cannot be read
     */
    List<String> databases() throws IOException;

    /**
     * Lists the tables of a database.
     *
     * @param database database name
     * @return table names, empty when the database does not exist
     * @throws IOException when the metastore cannot be read
     */
    List<String> tables(String database) throws IOException;

    /**
     * Reads some tables of a database.
     *
     * @param database database name
     * @param names table names
     * @return the tables that exist
     * @throws IOException when the metastore cannot be read
     */
    List<Table> tableObjects(String database, List<String> names) throws IOException;

    /**
     * Reads one table.
     *
     * @param database database name
     * @param name table name
     * @return the table, empty when it does not exist
     * @throws IOException when the metastore cannot be read
     */
    Optional<Table> table(String database, String name) throws IOException;

    /**
     * Lists the partition names of a table ({@code k1=v1/k2=v2}, escaped).
     *
     * @param database database name
     * @param table table name
     * @return partition names
     * @throws IOException when the metastore cannot be read
     */
    List<String> partitionNames(String database, String table) throws IOException;

    /**
     * Reads partitions by name.
     *
     * @param database database name
     * @param table table name
     * @param names partition names
     * @return the partitions that exist
     * @throws IOException when the metastore cannot be read
     */
    List<Partition> partitions(String database, String table, List<String> names)
            throws IOException;

    /**
     * Closes the connections.
     */
    @Override
    void close();
}
