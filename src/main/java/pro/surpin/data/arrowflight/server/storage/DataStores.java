package pro.surpin.data.arrowflight.server.storage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Opens the store of the data directory, chosen by the scheme of its URI: {@code s3://} (also
 * {@code s3a://} and {@code s3n://}) is an object store, anything else (a plain path,
 * {@code hdfs://}, {@code file:}) a Hadoop file system.
 */
public final class DataStores {

    /** Schemes that name an S3 bucket. */
    private static final Set<String> S3_SCHEMES = Set.of("s3", "s3a", "s3n");
    /** A URI scheme followed by "//", which a Windows drive letter is not. */
    private static final Pattern SCHEME = Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*)://");

    /** Not instantiable. */
    private DataStores() {
    }

    /**
     * Names the store kind a data directory needs.
     *
     * @param dataDirectory data directory as configured
     * @return "s3" for an S3 bucket, "hadoop" otherwise
     */
    public static String kindOf(String dataDirectory) {
        if (dataDirectory != null) {
            Matcher scheme = SCHEME.matcher(dataDirectory.trim());
            if (scheme.find() && S3_SCHEMES.contains(scheme.group(1).toLowerCase(Locale.ROOT))) {
                return "s3";
            }
        }
        return "hadoop";
    }

    /**
     * Opens the store of the configured data directory.
     *
     * @param config server configuration
     * @param hadoopConfig Hadoop configuration, which receives the client read options
     * @param storageHost storage host colocated with this server
     * @return data store
     * @throws IllegalArgumentException when the S3 settings cannot reach an S3 data directory
     * @throws UncheckedIOException when the file system cannot be opened
     */
    public static DataStore open(AppConfig config, Configuration hadoopConfig, String storageHost) {
        String dataDirectory = config.dataDir();
        if ("s3".equals(kindOf(dataDirectory))) {
            return S3DataStore.open(config);
        }
        hadoopConfig.setInt("io.file.buffer.size", config.ioFileBufferSize());
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
