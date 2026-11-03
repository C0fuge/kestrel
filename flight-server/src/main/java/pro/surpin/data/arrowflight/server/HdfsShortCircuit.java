package pro.surpin.data.arrowflight.server;

import org.apache.hadoop.conf.Configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import pro.surpin.data.arrowflight.server.model.AppConfig;

/**
 * Short-circuit local reads of the hadoopfs extension. The extension hands libhdfs3 its own
 * {@code hdfs_shortcircuit} (default false) and {@code hdfs_domain_socket_path} (default empty)
 * settings, which win over {@code dfs.client.read.shortcircuit} and {@code dfs.domain.socket.path}
 * in the LIBHDFS3_CONF file: without both settings DuckDB reads every block over TCP. The server
 * therefore fills an unset {@code duckDbHdfsShortcircuit} / {@code duckDbHdfsDomainSocketPath}
 * from the LIBHDFS3_CONF file, then from the server's Hadoop configuration.
 */
final class HdfsShortCircuit {

    /** Client setting that turns short-circuit local reads on. */
    static final String SHORT_CIRCUIT = "dfs.client.read.shortcircuit";

    /** DataNode domain socket path. */
    static final String SOCKET_PATH = "dfs.domain.socket.path";

    /** SASL protection of the DataNode data-transfer port, which libhdfs3 does not speak. */
    static final String TRANSFER_PROTECTION = "dfs.data.transfer.protection";

    /**
     * Effective short-circuit settings of the extension.
     *
     * @param shortCircuit value for hdfs_shortcircuit, null when nothing sets it
     * @param socketPath value for hdfs_domain_socket_path, null when nothing sets it
     * @param source where the values come from
     * @param transferProtection the cluster's data-transfer protection, null when none is set
     */
    record Settings(String shortCircuit, String socketPath, String source,
            String transferProtection) {

        /**
         * Tells whether the extension reads short-circuit with these settings.
         *
         * @return true when short-circuit is on and a socket path is set
         */
        boolean enabled() {
            return "true".equalsIgnoreCase(shortCircuit) && socketPath != null
                    && !socketPath.isBlank();
        }

        /**
         * Returns a configuration whose short-circuit settings are these.
         *
         * @param config configuration
         * @return changed copy, or the configuration itself when nothing changes
         */
        AppConfig applyTo(AppConfig config) {
            if (java.util.Objects.equals(shortCircuit, config.duckDbHdfsShortcircuit())
                    && java.util.Objects.equals(socketPath, config.duckDbHdfsDomainSocketPath())) {
                return config;
            }
            return config.withHdfsShortCircuit(shortCircuit, socketPath);
        }

        /**
         * Describes the settings for the startup log.
         *
         * @return one line of key=value pairs
         */
        String describe() {
            return "enabled=" + enabled() + " shortcircuit=" + shortCircuit + " socket="
                    + socketPath + " source=" + source + " transferProtection="
                    + (transferProtection == null ? "none" : transferProtection);
        }
    }

    /** Not instantiable. */
    private HdfsShortCircuit() {
    }

    /**
     * Resolves the extension's short-circuit settings. An explicit server setting wins; an unset
     * one comes from the LIBHDFS3_CONF file, then from the Hadoop configuration, where a socket
     * path without {@code dfs.client.read.shortcircuit} turns short-circuit on (as the server's
     * own HDFS client does it).
     *
     * @param shortCircuit duckDbHdfsShortcircuit, null when unset
     * @param socketPath duckDbHdfsDomainSocketPath, null when unset
     * @param libhdfs3 properties of the LIBHDFS3_CONF file, empty when there is none
     * @param hadoopConfig the server's Hadoop configuration, or null
     * @return effective settings
     */
    static Settings resolve(String shortCircuit, String socketPath, Map<String, String> libhdfs3,
            Configuration hadoopConfig) {
        String hadoopPath = hadoopConfig == null ? null
                : blankToNull(hadoopConfig.getTrimmed(SOCKET_PATH));
        String hadoopShortCircuit = null;
        if (hadoopConfig != null) {
            hadoopShortCircuit = hadoopConfig.get(SHORT_CIRCUIT) != null
                    && !onlyDefault(hadoopConfig, SHORT_CIRCUIT)
                    ? hadoopConfig.getTrimmed(SHORT_CIRCUIT)
                    : hadoopPath == null ? null : "true";
        }
        List<String> sources = new ArrayList<>();
        String sc = pick(shortCircuit, blankToNull(libhdfs3.get(SHORT_CIRCUIT)),
                hadoopShortCircuit, sources);
        String path = pick(socketPath, blankToNull(libhdfs3.get(SOCKET_PATH)), hadoopPath,
                sources);
        String protection = blankToNull(libhdfs3.get(TRANSFER_PROTECTION));
        if (protection == null && hadoopConfig != null) {
            protection = blankToNull(hadoopConfig.getTrimmed(TRANSFER_PROTECTION));
        }
        String source = sources.isEmpty() ? "none"
                : String.join("+", new java.util.LinkedHashSet<>(sources));
        return new Settings(sc, path, source, protection);
    }

    /**
     * Finds what keeps DuckDB from reading this node's HDFS data: DataNodes that need SASL
     * data-transfer protection serve libhdfs3 only through the local domain socket.
     *
     * @param settings effective settings
     * @return the reason to stop the start, empty when reads can work
     */
    static Optional<String> unreadable(Settings settings) {
        if (settings.transferProtection() == null) {
            return Optional.empty();
        }
        String head = "HDFS DataNodes require SASL data-transfer protection ("
                + TRANSFER_PROTECTION + "=" + settings.transferProtection()
                + "), which the hadoopfs extension (libhdfs3) cannot speak: DuckDB can read HDFS"
                + " blocks only short-circuit, through the local DataNode's domain socket";
        if (!settings.enabled()) {
            return Optional.of(head + ", but short-circuit is off (shortcircuit="
                    + settings.shortCircuit() + ", socket=" + settings.socketPath() + "). Set "
                    + SHORT_CIRCUIT + "=true and " + SOCKET_PATH + " in the LIBHDFS3_CONF file,"
                    + " or duckDbHdfsShortcircuit=true and duckDbHdfsDomainSocketPath.");
        }
        if (!socketPresent(settings.socketPath())) {
            return Optional.of(head + ", but the socket " + settings.socketPath()
                    + " does not exist on this node. Run the server on a DataNode host with the"
                    + " DataNode's socket visible to it.");
        }
        return Optional.empty();
    }

    /**
     * Tells whether a configured domain socket exists. A path with the DataNode port pattern
     * ({@code _PORT}) cannot be checked and counts as present.
     *
     * @param socketPath configured path
     * @return false when the path names no file
     */
    static boolean socketPresent(String socketPath) {
        return socketPath.contains("_PORT") || Files.exists(Path.of(socketPath));
    }

    /**
     * Takes the first non-null value and records its source.
     *
     * @param explicit server setting
     * @param libhdfs3 LIBHDFS3_CONF value
     * @param hadoop Hadoop configuration value
     * @param sources receives the source name
     * @return chosen value, or null
     */
    private static String pick(String explicit, String libhdfs3, String hadoop,
            List<String> sources) {
        if (explicit != null) {
            sources.add("server");
            return explicit;
        }
        if (libhdfs3 != null) {
            sources.add("LIBHDFS3_CONF");
            return libhdfs3;
        }
        if (hadoop != null) {
            sources.add("hadoop");
        }
        return hadoop;
    }

    /**
     * Tells whether only Hadoop's bundled default files set a property.
     *
     * @param hadoopConfig configuration
     * @param name property name
     * @return true when every source is a {@code *-default.xml}
     */
    private static boolean onlyDefault(Configuration hadoopConfig, String name) {
        String[] sources = hadoopConfig.getPropertySources(name);
        if (sources == null || sources.length == 0) {
            return false;
        }
        for (String source : sources) {
            if (!source.endsWith("-default.xml")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Maps a blank string to null.
     *
     * @param value value
     * @return trimmed value, or null when blank
     */
    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
