package pro.surpin.data.arrowflight.server.catalog;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Writes the Iceberg test tables of the iceberg plugin module (IcebergFixtures, Iceberg Java with
 * its Parquet 1.17 writer) from the server's tests: in a child-first class loader over
 * flight-plugin-iceberg/target/fixture-libs and its test classes, sharing only the JDK, Hadoop
 * and slf4j with this test class path (whose Parquet is the server's 1.13).
 */
public final class IcebergTestTables {

    /** The built plugins/iceberg directory. */
    public static final Path PLUGIN = Path.of(System.getProperty("iceberg.it.pluginDir",
            "../flight-plugin-iceberg/target/plugin/iceberg"));
    private static final Path MODULE = Path.of("../flight-plugin-iceberg/target");
    private static final String FIXTURES = "pro.surpin.data.arrowflight.plugin.iceberg.IcebergFixtures";
    private static final String REST = "pro.surpin.data.arrowflight.plugin.iceberg.FakeRestCatalog";
    private static ClassLoader loader;

    private IcebergTestTables() {
    }

    /**
     * Returns the directory with v&lt;version&gt;/&lt;platform&gt;/{iceberg,avro}.duckdb_extension
     * ({@code -Diceberg.it.extensionDir}), when it has the files for DuckDB 1.4.5 on linux_amd64.
     *
     * @return directory, or null to skip the tests that need DuckDB's iceberg extension
     */
    public static String extensionDir() {
        String dir = System.getProperty("iceberg.it.extensionDir", "");
        if (dir.isBlank()) {
            return null;
        }
        Path files = Path.of(dir, "v1.4.5", "linux_amd64");
        return Files.isRegularFile(files.resolve("iceberg.duckdb_extension"))
                && Files.isRegularFile(files.resolve("avro.duckdb_extension")) ? dir : null;
    }

    /**
     * Tells whether the plugin module was built (its plugin directory and fixture libraries).
     *
     * @return true when the tables can be written
     */
    public static boolean available() {
        return Files.isDirectory(PLUGIN) && Files.isDirectory(MODULE.resolve("fixture-libs"))
                && Files.isDirectory(MODULE.resolve("test-classes"));
    }

    /**
     * Writes all test tables into a warehouse directory.
     *
     * @param warehouse directory
     * @return table name to its current metadata file
     * @throws Exception when the tables cannot be written
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> write(String warehouse) throws Exception {
        return (Map<String, String>) call(FIXTURES, "write", warehouse);
    }

    /**
     * Commits two more rows to table evolved.
     *
     * @param warehouse directory
     * @param firstId first new id
     * @return new metadata file
     * @throws Exception when the rows cannot be written
     */
    public static String appendToEvolved(String warehouse, long firstId) throws Exception {
        return (String) call(FIXTURES, "appendToEvolved", warehouse, firstId);
    }

    /**
     * Starts a fake Iceberg REST catalog over a warehouse.
     *
     * @param warehouse directory
     * @return the catalog; its URI from {@link #uri(AutoCloseable)}
     * @throws Exception when it cannot start
     */
    public static AutoCloseable rest(String warehouse) throws Exception {
        return (AutoCloseable) call(REST, "start", warehouse);
    }

    /**
     * Returns the URI of a fake REST catalog.
     *
     * @param rest catalog from {@link #rest(String)}
     * @return URI
     * @throws Exception on reflection failure
     */
    public static String uri(AutoCloseable rest) throws Exception {
        return (String) rest.getClass().getMethod("uri").invoke(rest);
    }

    /**
     * Copies the built plugin into a plugins directory.
     *
     * @param plugins plugins directory
     * @throws IOException when it cannot be copied
     */
    public static void installPlugin(Path plugins) throws IOException {
        Path target = Files.createDirectories(plugins.resolve("iceberg"));
        try (Stream<Path> jars = Files.list(PLUGIN)) {
            for (Path jar : jars.toList()) {
                Files.copy(jar, target.resolve(jar.getFileName().toString()));
            }
        }
    }

    private static Object call(String className, String method, Object... args) throws Exception {
        ClassLoader child = loader();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(child);
        try {
            Class<?> type = Class.forName(className, true, child);
            for (java.lang.reflect.Method m : type.getMethods()) {
                if (m.getName().equals(method) && m.getParameterCount() == args.length) {
                    return m.invoke(null, args);
                }
            }
            throw new NoSuchMethodException(className + "." + method);
        } catch (InvocationTargetException e) {
            throw e.getCause() instanceof Exception ex ? ex : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static synchronized ClassLoader loader() throws IOException {
        if (loader == null) {
            List<URL> urls = new ArrayList<>();
            urls.add(url(MODULE.resolve("test-classes")));
            try (Stream<Path> jars = Files.list(MODULE.resolve("fixture-libs"))) {
                for (Path jar : jars.sorted().toList()) {
                    urls.add(url(jar));
                }
            }
            loader = new ChildFirst(urls.toArray(new URL[0]), IcebergTestTables.class.getClassLoader());
        }
        return loader;
    }

    private static URL url(Path path) throws MalformedURLException {
        return path.toAbsolutePath().toUri().toURL();
    }

    /** Child-first except the JDK, Hadoop and slf4j. */
    private static final class ChildFirst extends URLClassLoader {
        ChildFirst(URL[] urls, ClassLoader parent) {
            super("iceberg-fixtures", urls, parent);
        }

        private static boolean parentFirst(String name) {
            return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.")
                    || name.startsWith("sun.") || name.startsWith("com.sun.")
                    || name.startsWith("org.w3c.") || name.startsWith("org.xml.")
                    || name.startsWith("org.apache.hadoop.") || name.startsWith("org.slf4j.");
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null && !parentFirst(name)) {
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        c = null;
                    }
                }
                if (c == null) {
                    c = getParent().loadClass(name);
                }
                if (resolve) {
                    resolveClass(c);
                }
                return c;
            }
        }

        @Override
        public URL getResource(String name) {
            URL url = findResource(name);
            return url != null ? url : super.getResource(name);
        }
    }
}
