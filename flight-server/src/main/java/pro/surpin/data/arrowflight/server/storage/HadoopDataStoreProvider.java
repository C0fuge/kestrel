package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * The built-in store for {@code hdfs://}, {@code file:} and plain paths, and for any scheme no
 * other provider claims: a Hadoop file system of the cluster's Hadoop. It needs the server's
 * Hadoop configuration, so it is not an SPI provider.
 */
public final class HadoopDataStoreProvider {

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
        hadoopConfig.setInt("io.file.buffer.size", ioFileBufferSize);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit", true);
        hadoopConfig.setBoolean("dfs.client.read.shortcircuit.skip.checksum", false);
        hadoopConfig.setBoolean("fs.file.impl.disable.cache", true);
        try {
            FileSystem fileSystem = new Path(dataDirectory).getFileSystem(hadoopConfig);
            return new HadoopDataStore(fileSystem, dataDirectory, storageHost);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
