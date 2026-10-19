package pro.surpin.data.arrowflight.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.storage.DataStore;
import pro.surpin.data.arrowflight.server.storage.HadoopDataStore;

import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.net.URI;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Golden test (a) of the plugin plan: the statements every DuckDB connection runs, in order, for
 * the HDFS stand's configuration, a local data directory and each S3 variant. The configuration
 * goes through {@link ConfigAdapter} from the same -D flags the bench uses, and the store and the
 * DuckDB adapter come from {@link ServerModule}, so a refactoring of any of them that changes a
 * single statement fails here. DuckDB itself is replaced by a JDBC driver that records the
 * statements, so no extension has to exist.
 */
@Tag("unit")
class GoldenDuckDbSetupTest {

    private static final String DUCKDB_URL = "jdbc:duckdb:";

    @TempDir
    Path dir;

    /** Verifies the setup statements of every storage configuration against the golden file. */
    @Test
    void setupStatementsOfEveryStorageConfiguration() throws Exception {
        Path httpfs = Files.writeString(dir.resolve("httpfs.duckdb_extension"), "not an extension");
        Path credentials = dir.resolve("s3.credentials");
        Files.writeString(credentials, "accessKey=AKIAGOLDEN\nsecretKey=golden'secret\n");
        Files.setPosixFilePermissions(credentials, PosixFilePermissions.fromString("rw-------"));
        Path tokenCredentials = dir.resolve("s3-token.credentials");
        Files.writeString(tokenCredentials,
                "accessKey=ASIAGOLDEN\nsecretKey=goldensecret2\nsessionToken=golden-token\n");
        Files.setPosixFilePermissions(tokenCredentials, PosixFilePermissions.fromString("rw-------"));
        Path ca = dir.resolve("ca.pem");
        try (InputStream in = getClass().getResourceAsStream("/golden-input/test-ca.pem")) {
            Files.copy(in, ca, StandardCopyOption.REPLACE_EXISTING);
        }
        Path local = Files.createDirectories(dir.resolve("local/parquet"));
        Path temp = Files.createDirectories(dir.resolve("duckdb-tmp"));

        Map<String, String> common = new LinkedHashMap<>();
        common.put("duckDbWarmConnections", "0");
        common.put("duckDbTempDirectory", temp.toString());
        common.put("ioParallelism", "8");
        common.put("duckDbMemoryLimit", "1GB");
        common.put("duckDbThreads", "2");

        Map<String, String> hdfs = new LinkedHashMap<>(common);
        hdfs.put("numServers", "1");
        hdfs.put("dataDir", "hdfs://hdfs-ns1/data/parquet");
        hdfs.put("duckDbHdfsExtension", "/ext/hadoopfs.duckdb_extension");
        hdfs.put("duckDbAllowUnsignedExtensions", "true");
        hdfs.put("duckDbHdfsDefaultNamenode", "hdfs-ns1");
        hdfs.put("duckDbHdfsHaNamenodes", "nn1=nn:8020,nn2=nn2:8020");
        hdfs.put("duckDbHdfsShortcircuit", "true");
        hdfs.put("duckDbHdfsDomainSocketPath", "/var/lib/hadoop-hdfs/dn_socket");

        Map<String, String> localDir = new LinkedHashMap<>(common);
        localDir.put("dataDir", local.toString());
        localDir.put("duckDbStreamResults", "false");
        localDir.put("duckDbExternalFileCache", "true");

        Map<String, String> s3Path = new LinkedHashMap<>(common);
        s3Path.put("dataDir", "s3://lake/parquet");
        s3Path.put("s3Endpoint", "http://minio:9000");
        s3Path.put("s3CredentialsFile", credentials.toString());
        s3Path.put("duckDbHttpfsExtension", httpfs.toString());

        Map<String, String> s3Vhost = new LinkedHashMap<>(common);
        s3Vhost.put("dataDir", "s3a://lake/parquet/");
        s3Vhost.put("s3Endpoint", "https://s3.example.com");
        s3Vhost.put("s3Region", "eu-west-3");
        s3Vhost.put("s3UrlStyle", "vhost");
        s3Vhost.put("s3CredentialsFile", tokenCredentials.toString());
        s3Vhost.put("duckDbHttpfsExtension", httpfs.toString());

        Map<String, String> s3TlsCa = new LinkedHashMap<>(common);
        s3TlsCa.put("dataDir", "s3://lake/parquet");
        s3TlsCa.put("s3Endpoint", "https://minio:9443");
        s3TlsCa.put("s3CaFile", ca.toString());
        s3TlsCa.put("s3CredentialsFile", credentials.toString());
        s3TlsCa.put("duckDbHttpfsExtension", httpfs.toString());

        Map<String, String> s3TlsNoVerify = new LinkedHashMap<>(s3TlsCa);
        s3TlsNoVerify.remove("s3CaFile");
        s3TlsNoVerify.put("s3VerifyTls", "false");

        Map<String, String> s3NoCache = new LinkedHashMap<>(s3Path);
        s3NoCache.put("duckDbS3MetadataCache", "false");
        s3NoCache.put("dataDir", "s3n://lake");

        Map<String, String> s3NoLockdown = new LinkedHashMap<>(s3Path);
        s3NoLockdown.put("duckDbLockdown", "false");

        StringBuilder out = new StringBuilder();
        Map<String, Map<String, String>> configs = new LinkedHashMap<>();
        configs.put("hdfs", hdfs);
        configs.put("local", localDir);
        configs.put("s3-path", s3Path);
        configs.put("s3-vhost-token", s3Vhost);
        configs.put("s3-tls-ca", s3TlsCa);
        configs.put("s3-tls-noverify", s3TlsNoVerify);
        configs.put("s3-metadata-cache-off", s3NoCache);
        configs.put("s3-no-lockdown", s3NoLockdown);
        for (Map.Entry<String, Map<String, String>> config : configs.entrySet()) {
            out.append("# config ").append(config.getKey()).append('\n');
            for (Map.Entry<String, String> property : config.getValue().entrySet()) {
                out.append("# -D").append(property.getKey()).append('=')
                        .append(property.getValue()).append('\n');
            }
            out.append(setup(config.getValue())).append('\n');
        }
        String text = out.toString()
                .replace(httpfs.toString(), "<TMP>/httpfs.duckdb_extension")
                .replace(dir.toString(), "<TMP>");
        text = Pattern.compile("<TMP>/duckdb-tmp/duckdb-[0-9]+-[0-9]+").matcher(text)
                .replaceAll(Matcher.quoteReplacement("<TMP>/duckdb-tmp/<RUN>"));
        Golden.assertMatches("duckdb-setup.txt", text);
    }

    /**
     * Builds the server's data store and DuckDB adapter from -D flags and records the
     * connections the adapter opens: the startup check, then one query connection.
     *
     * @param flags system properties as passed with -D
     * @return recorded connections and statements
     * @throws Exception when the configuration is refused
     */
    private static String setup(Map<String, String> flags) throws Exception {
        Map<String, String> saved = new LinkedHashMap<>();
        for (Map.Entry<String, String> flag : flags.entrySet()) {
            saved.put(flag.getKey(), System.getProperty(flag.getKey()));
            System.setProperty(flag.getKey(), flag.getValue());
        }
        List<Driver> duckDrivers = new ArrayList<>();
        RecordingDriver recorder = new RecordingDriver();
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        try {
            AppConfig config = ConfigAdapter.getConfig();
            ServerModule module = new ServerModule(new String[] {"localhost"},
                    "grpc://localhost:32010", "localhost", new Configuration());
            for (Driver driver : Collections.list(DriverManager.getDrivers())) {
                if (driver.acceptsURL(DUCKDB_URL)) {
                    duckDrivers.add(driver);
                    DriverManager.deregisterDriver(driver);
                }
            }
            DriverManager.registerDriver(recorder);
            StringBuilder out = new StringBuilder();
            // Hadoop 3.3's HDFS client needs Subject.getSubject, which JDK 23+ refuses; the
            // HDFS store gets a file system that only qualifies paths, as HDFS would.
            DataStore store = config.dataDir().startsWith("hdfs://")
                    ? new HadoopDataStore(hdfsFileSystem(config.dataDir()), config.dataDir(), "localhost")
                    : module.dataStore(config);
            try {
                out.append("store kind=").append(store.kind()).append(" roots=")
                        .append(store.duckDbRoots()).append('\n');
                try (DuckDbAdapter adapter = module.duckDb(config, ioPool, store)) {
                    adapter.connection();
                }
            } finally {
                if (store instanceof AutoCloseable closeable) {
                    closeable.close();
                }
            }
            out.append(String.join("\n", recorder.log)).append('\n');
            return out.toString();
        } finally {
            DriverManager.deregisterDriver(recorder);
            for (Driver driver : duckDrivers) {
                DriverManager.registerDriver(driver);
            }
            ioPool.shutdownNow();
            for (Map.Entry<String, String> flag : saved.entrySet()) {
                if (flag.getValue() == null) {
                    System.clearProperty(flag.getKey());
                } else {
                    System.setProperty(flag.getKey(), flag.getValue());
                }
            }
        }
    }

    /**
     * Builds a file system of an HDFS nameservice that only names its URI and qualifies paths.
     *
     * @param dataDir data directory on HDFS
     * @return file system
     */
    private static FileSystem hdfsFileSystem(String dataDir) {
        URI uri = URI.create(dataDir).resolve("/");
        FileSystem fs = mock(FileSystem.class);
        when(fs.getUri()).thenReturn(uri);
        when(fs.makeQualified(any(org.apache.hadoop.fs.Path.class))).thenAnswer(inv ->
                ((org.apache.hadoop.fs.Path) inv.getArgument(0)).makeQualified(uri,
                        new org.apache.hadoop.fs.Path("/")));
        return fs;
    }

    /** JDBC driver for {@code jdbc:duckdb:} that records what is run instead of running it. */
    private static final class RecordingDriver implements Driver {

        private final List<String> log = new ArrayList<>();
        private int connections;

        @Override
        public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) {
                return null;
            }
            Map<String, String> properties = new TreeMap<>();
            for (String name : info.stringPropertyNames()) {
                properties.put(name, info.getProperty(name));
            }
            log.add("-- connection " + (++connections) + " " + url + " " + properties);
            InvocationHandler resultSet = (proxy, method, args) -> defaultValue(method.getReturnType());
            InvocationHandler statement = (proxy, method, args) -> switch (method.getName()) {
                case "execute" -> {
                    log.add((String) args[0]);
                    yield false;
                }
                case "executeQuery" -> {
                    log.add("QUERY " + args[0]);
                    yield Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[] {ResultSet.class}, resultSet);
                }
                default -> defaultValue(method.getReturnType());
            };
            InvocationHandler connection = (proxy, method, args) -> switch (method.getName()) {
                case "createStatement" -> Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] {Statement.class}, statement);
                case "prepareStatement" -> throw new UnsupportedOperationException(
                        "prepareStatement " + args[0]);
                default -> defaultValue(method.getReturnType());
            };
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] {Connection.class}, connection);
        }

        /**
         * Returns the value a proxy method returns when it does nothing.
         *
         * @param type return type
         * @return zero, false or null
         */
        private static Object defaultValue(Class<?> type) {
            if (type == boolean.class) {
                return false;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            return null;
        }

        @Override
        public boolean acceptsURL(String url) {
            return url != null && url.startsWith(DUCKDB_URL);
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 0;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }
    }
}
