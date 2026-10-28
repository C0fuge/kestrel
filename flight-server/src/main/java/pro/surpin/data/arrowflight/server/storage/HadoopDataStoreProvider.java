package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The built-in store for {@code hdfs://}, {@code file:} and plain paths, and for any scheme no
 * other provider claims: a Hadoop file system of the cluster's Hadoop. It needs the server's
 * Hadoop configuration, so it is not an SPI provider.
 */
public final class HadoopDataStoreProvider {

    /** Hadoop client setting for short-circuit local reads. */
    private static final String SHORT_CIRCUIT = "dfs.client.read.shortcircuit";

    /** Not instantiable. */
    private HadoopDataStoreProvider() {
    }

    /**
     * Opens the store of a data directory, setting the Hadoop client's read options first.
     *
     * @param dataDirectory data directory as configured
     * @param hadoopConfig Hadoop configuration, which receives the client read options
     * @param ioFileBufferSize Hadoop I/O buffer size
     * @param storageHost storage host colocated with this server
     * @return store
     * @throws UncheckedIOException when the file system cannot be opened
     */
    public static HadoopDataStore open(String dataDirectory, Configuration hadoopConfig,
            int ioFileBufferSize, String storageHost) {
        return open(dataDirectory, hadoopConfig, ioFileBufferSize, storageHost, false);
    }

    /**
     * Opens the store of a data directory, setting the Hadoop client's read options first.
     *
     * @param dataDirectory data directory as configured
     * @param hadoopConfig Hadoop configuration, which receives the client read options
     * @param ioFileBufferSize Hadoop I/O buffer size
     * @param storageHost storage host colocated with this server
     * @param sharedMount whether a local data directory is a mount every node reads
     * @return store
     * @throws UncheckedIOException when the file system cannot be opened
     */
    public static HadoopDataStore open(String dataDirectory, Configuration hadoopConfig,
            int ioFileBufferSize, String storageHost, boolean sharedMount) {
        hadoopConfig.setInt("io.file.buffer.size", ioFileBufferSize);
        applyShortCircuit(hadoopConfig);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
        hadoopConfig.setBoolean("fs.file.impl.disable.cache", true);
        registerFileSystems(hadoopConfig);
        try {
            FileSystem fileSystem = new Path(dataDirectory).getFileSystem(hadoopConfig);
            return new HadoopDataStore(fileSystem, dataDirectory, storageHost, sharedMount);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Turns HDFS short-circuit reads on when the deployment configures a DataNode domain socket
     * and does not set {@code dfs.client.read.shortcircuit} itself. A value from the deployment's
     * configuration (hdfs-site.xml or -D) is kept; without a socket path short-circuit stays off,
     * since the Hadoop client refuses to start with it on and no socket.
     *
     * @param hadoopConfig configuration to complete
     */
    static void applyShortCircuit(Configuration hadoopConfig) {
        if (explicit(hadoopConfig, SHORT_CIRCUIT)) {
            return;
        }
        hadoopConfig.setBoolean(SHORT_CIRCUIT,
                !hadoopConfig.getTrimmed("dfs.domain.socket.path", "").isEmpty());
    }

    /**
     * Tells whether a configuration sets a property itself rather than through Hadoop's bundled
     * {@code *-default.xml} files.
     *
     * @param hadoopConfig configuration
     * @param name property name
     * @return true when a non-default source sets it
     */
    static boolean explicit(Configuration hadoopConfig, String name) {
        if (hadoopConfig.get(name) == null) {
            return false;
        }
        String[] sources = hadoopConfig.getPropertySources(name);
        if (sources == null) {
            return true;
        }
        for (String source : sources) {
            if (!source.endsWith("-default.xml")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Names the file system classes of {@code file:} and {@code hdfs:} unless the configuration
     * names them already. Hadoop otherwise finds them only through the
     * {@code META-INF/services/org.apache.hadoop.fs.FileSystem} files of its jars, which merged or
     * repackaged Hadoop class paths lose ("No FileSystem for scheme "file"" at startup with a
     * local data directory or warehouse). An explicit {@code fs.<scheme>.impl} (core-site.xml or
     * -D) is kept.
     *
     * @param hadoopConfig configuration to complete
     */
    static void registerFileSystems(Configuration hadoopConfig) {
        if (hadoopConfig.get("fs.file.impl") == null) {
            hadoopConfig.set("fs.file.impl", LocalFileSystem.class.getName());
        }
        String hdfs = "org.apache.hadoop.hdfs.DistributedFileSystem";
        if (hadoopConfig.get("fs.hdfs.impl") == null && present(hdfs)) {
            hadoopConfig.set("fs.hdfs.impl", hdfs);
        }
    }

    /**
     * Tells whether a class is on the server's class path.
     *
     * @param name class name
     * @return true when it can be loaded
     */
    private static boolean present(String name) {
        try {
            Class.forName(name, false, HadoopDataStoreProvider.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }
}
