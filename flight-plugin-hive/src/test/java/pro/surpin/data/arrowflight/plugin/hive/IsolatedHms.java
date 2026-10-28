package pro.surpin.data.arrowflight.plugin.hive;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
 * Runs {@link HmsFixture} for the plugin's tests in a child-first class loader over
 * target/fixture-libs (Hive 3.1.3 with the libthrift 0.9.3 its server needs) and the test
 * classes, sharing only the JDK, Hadoop and slf4j with this class path, where the plugin runs on
 * its own libthrift. The server's tests do the same (HiveTestTables).
 */
final class IsolatedHms {

    private static final Path MODULE = Path.of("target");
    private static final String FIXTURE = "pro.surpin.data.arrowflight.plugin.hive.HmsFixture";
    private static ClassLoader loader;
    private static IsolatedHms shared;

    private final Object fixture;

    private IsolatedHms(Object fixture) {
        this.fixture = fixture;
    }

    /**
     * Starts the metastore once per JVM.
     *
     * @return the running metastore
     * @throws Exception when it does not start
     */
    static synchronized IsolatedHms shared() throws Exception {
        if (shared == null) {
            ClassLoader child = loader();
            shared = new IsolatedHms(inLoader(() -> Class.forName(FIXTURE, true, child)
                    .getMethod("shared").invoke(null)));
        }
        return shared;
    }

    /**
     * Returns the class loader the metastore runs in.
     *
     * @return class loader
     * @throws IOException when the fixture libraries cannot be listed
     */
    static synchronized ClassLoader loader() throws IOException {
        if (loader == null) {
            List<URL> urls = new ArrayList<>();
            urls.add(url(MODULE.resolve("test-classes")));
            try (Stream<Path> jars = Files.list(MODULE.resolve("fixture-libs"))) {
                for (Path jar : jars.sorted().toList()) {
                    urls.add(url(jar));
                }
            }
            loader = new ChildFirst(urls.toArray(new URL[0]), IsolatedHms.class.getClassLoader());
        }
        return loader;
    }

    /**
     * Names the metastore.
     *
     * @return {@code thrift://127.0.0.1:<port>}
     * @throws Exception on failure
     */
    String uri() throws Exception {
        return (String) call(fixture, "uri");
    }

    /**
     * Creates a database unless it exists.
     *
     * @param name database name
     * @throws Exception on metastore errors
     */
    void createDatabase(String name) throws Exception {
        call(fixture, "createDatabase", name);
    }

    /**
     * Creates a table, see {@link HmsFixture#createTable}.
     *
     * @param db database
     * @param name table name
     * @param location table location
     * @param columns columns as {@code name type;...}
     * @param partitionColumns partition columns, same form
     * @param format parquet, text or orc
     * @param parameters table parameters, or null
     * @throws Exception on metastore errors
     */
    void createTable(String db, String name, String location, String columns,
            String partitionColumns, String format, Map<String, String> parameters) throws Exception {
        call(fixture, "createTable", db, name, location, columns, partitionColumns, format, parameters);
    }

    /**
     * Creates a view.
     *
     * @param db database
     * @param name view name
     * @param sql view text
     * @throws Exception on metastore errors
     */
    void createView(String db, String name, String sql) throws Exception {
        call(fixture, "createView", db, name, sql);
    }

    /**
     * Adds a partition.
     *
     * @param db database
     * @param table table name
     * @param values partition values
     * @param location partition location, or null for the default
     * @return the partition's location as the metastore stores it
     * @throws Exception on metastore errors
     */
    String addPartition(String db, String table, List<String> values, String location)
            throws Exception {
        return (String) call(fixture, "addPartition", db, table, values, location);
    }

    /**
     * Adds a partition with serde parameters of its own.
     *
     * @param db database
     * @param table table name
     * @param values partition values
     * @param location partition location, or null for the default
     * @param serdeParameters the partition's serde parameters (replacing the table's)
     * @return the partition's location as the metastore stores it
     * @throws Exception on metastore errors
     */
    String addPartition(String db, String table, List<String> values, String location,
            Map<String, String> serdeParameters) throws Exception {
        return (String) call(fixture, "addPartition", db, table, values, location,
                serdeParameters);
    }

    /**
     * Starts a TCP proxy in front of the metastore.
     *
     * @return the proxy
     * @throws Exception on failure
     */
    Proxy proxy() throws Exception {
        return new Proxy((AutoCloseable) call(fixture, "proxy"));
    }

    /**
     * Starts a proxy that speaks the framed transport to its clients.
     *
     * @return the proxy
     * @throws Exception on failure
     */
    Proxy framedProxy() throws Exception {
        return new Proxy((AutoCloseable) call(fixture, "framedProxy"));
    }

    /** A TCP proxy to the metastore; closing it cuts every connection. */
    static final class Proxy implements AutoCloseable {
        private final AutoCloseable proxy;

        Proxy(AutoCloseable proxy) {
            this.proxy = proxy;
        }

        /**
         * Names the proxy.
         *
         * @return {@code thrift://127.0.0.1:<port>}
         * @throws Exception on failure
         */
        String uri() throws Exception {
            return (String) call(proxy, "uri");
        }

        @Override
        public void close() throws Exception {
            proxy.close();
        }
    }

    /** A call that may throw. */
    @FunctionalInterface
    private interface Call {
        Object run() throws Exception;
    }

    private static Object call(Object target, String method, Object... args) throws Exception {
        for (Method m : target.getClass().getMethods()) {
            if (m.getName().equals(method) && m.getParameterCount() == args.length) {
                return inLoader(() -> m.invoke(target, args));
            }
        }
        throw new NoSuchMethodException(method);
    }

    private static Object inLoader(Call call) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader());
        try {
            return call.run();
        } catch (InvocationTargetException e) {
            throw e.getCause() instanceof Exception ex ? ex : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    private static URL url(Path path) throws MalformedURLException {
        return path.toAbsolutePath().toUri().toURL();
    }

    /** Child-first except the JDK, Hadoop (not Hive) and slf4j. */
    private static final class ChildFirst extends URLClassLoader {
        ChildFirst(URL[] urls, ClassLoader parent) {
            super("hms-fixture", urls, parent);
        }

        private static boolean parentFirst(String name) {
            if (name.startsWith("javax.jdo.") || name.startsWith("javax.transaction.")
                    || name.startsWith("org.apache.hadoop.hive.")) {
                return false;
            }
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
