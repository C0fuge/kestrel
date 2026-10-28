package pro.surpin.data.arrowflight.server.catalog;

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
import java.util.stream.Stream;

/**
 * Runs the hive plugin module's in-process metastore (HmsFixture: Hive 3.1.3 on embedded Derby)
 * from the server's tests, in a child-first class loader over flight-plugin-hive/target/fixture-libs
 * and its test classes, sharing only the JDK, Hadoop and slf4j with this test class path.
 */
public final class HiveTestTables {

    /** The built plugins/hive directory. */
    public static final Path PLUGIN = Path.of(System.getProperty("hive.it.pluginDir",
            "../flight-plugin-hive/target/plugin/hive"));
    private static final Path MODULE = Path.of("../flight-plugin-hive/target");
    private static final String FIXTURE = "pro.surpin.data.arrowflight.plugin.hive.HmsFixture";
    private static final String ORC = "pro.surpin.data.arrowflight.plugin.hive.OrcTestFiles";
    private static final String AVRO = "pro.surpin.data.arrowflight.plugin.hive.AvroTestFiles";
    private static ClassLoader loader;

    private HiveTestTables() {
    }

    /**
     * Tells whether the plugin module was built (its plugin directory and fixture libraries).
     *
     * @return true when the metastore can run
     */
    public static boolean available() {
        return Files.isDirectory(PLUGIN) && Files.isDirectory(MODULE.resolve("fixture-libs"))
                && Files.isDirectory(MODULE.resolve("test-classes"));
    }

    /**
     * Starts (once per JVM) the metastore.
     *
     * @return a handle to call it
     * @throws Exception when it does not start
     */
    public static Hms hms() throws Exception {
        ClassLoader child = loader();
        Object fixture = inLoader(() -> Class.forName(FIXTURE, true, child).getMethod("shared")
                .invoke(null));
        return new Hms(fixture);
    }

    /**
     * Writes rows [from, to) of the plugin module's ORC test table as an ORC file (OrcTestFiles,
     * in the metastore's class loader, where orc-core is).
     *
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @param compression ORC compression kind
     * @throws Exception when it cannot be written
     */
    public static void writeOrc(Path file, int from, int to, String compression) throws Exception {
        ClassLoader child = loader();
        inLoader(() -> Class.forName(ORC, true, child).getMethod("write", String.class, int.class,
                int.class, String.class).invoke(null, file.toString(), from, to, compression));
    }

    /**
     * Writes rows [from, to) of the plugin module's lineitem-like table as an ORC file
     * (OrcTestFiles.writeLineitem, in the metastore's class loader).
     *
     * @param file file to write
     * @param from first row
     * @param to row after the last
     * @param compression ORC compression kind
     * @throws Exception when it cannot be written
     */
    public static void writeLineitemOrc(Path file, long from, long to, String compression)
            throws Exception {
        ClassLoader child = loader();
        inLoader(() -> Class.forName(ORC, true, child).getMethod("writeLineitem", String.class,
                long.class, long.class, String.class).invoke(null, file.toString(), from, to,
                compression));
    }

    /**
     * Reads a constant of the ORC test files: COLUMNS (metastore notation) or EXPECTED (the
     * rows in DuckDB SQL, to be followed by {@code FROM range(from, to)}), LINEITEM_COLUMNS or
     * LINEITEM (followed by {@code FROM range(from, to) t(r)}).
     *
     * @param name constant name
     * @return its value
     * @throws Exception on reflection failure
     */
    public static String orcConstant(String name) throws Exception {
        ClassLoader child = loader();
        return (String) inLoader(() -> Class.forName(ORC, true, child).getField(name).get(null));
    }

    /**
     * Writes an Avro data file (AvroTestFiles, in the metastore's class loader, where Avro is),
     * with the writer time zone UTC.
     *
     * @param file file to write
     * @param schemaJson the file's schema
     * @param codec null, deflate or zstandard
     * @param rows records of plain Java values (numbers, strings, BigDecimal, lists, maps,
     *     {@code Object[]} for records)
     * @throws Exception when it cannot be written
     */
    public static void writeAvro(Path file, String schemaJson, String codec, List<Object[]> rows)
            throws Exception {
        ClassLoader child = loader();
        inLoader(() -> Class.forName(AVRO, true, child).getMethod("write", String.class,
                String.class, String.class, List.class).invoke(null, file.toString(), schemaJson,
                codec, rows));
    }

    /**
     * Copies the built plugin into a plugins directory.
     *
     * @param plugins plugins directory
     * @throws IOException when it cannot be copied
     */
    public static void installPlugin(Path plugins) throws IOException {
        Path target = Files.createDirectories(plugins.resolve("hive"));
        try (Stream<Path> jars = Files.list(PLUGIN)) {
            for (Path jar : jars.toList()) {
                Files.copy(jar, target.resolve(jar.getFileName().toString()));
            }
        }
    }

    /** Calls on the metastore fixture. */
    public static final class Hms {
        private final Object fixture;

        Hms(Object fixture) {
            this.fixture = fixture;
        }

        /**
         * Calls a method of the fixture by name and argument count.
         *
         * @param method method name
         * @param args arguments
         * @return result
         * @throws Exception what the method throws
         */
        public Object call(String method, Object... args) throws Exception {
            for (Method m : fixture.getClass().getMethods()) {
                if (m.getName().equals(method) && m.getParameterCount() == args.length) {
                    return inLoader(() -> m.invoke(fixture, args));
                }
            }
            throw new NoSuchMethodException(method);
        }

        /**
         * Names the metastore.
         *
         * @return thrift URI
         * @throws Exception on reflection failure
         */
        public String uri() throws Exception {
            return (String) call("uri");
        }

        /**
         * Starts a TCP proxy in front of the metastore.
         *
         * @return the proxy (close cuts it off)
         * @throws Exception on failure
         */
        public AutoCloseable proxy() throws Exception {
            return (AutoCloseable) call("proxy");
        }
    }

    /**
     * Names a proxy.
     *
     * @param proxy proxy from {@link Hms#proxy()}
     * @return thrift URI
     * @throws Exception on reflection failure
     */
    public static String uri(AutoCloseable proxy) throws Exception {
        return (String) proxy.getClass().getMethod("uri").invoke(proxy);
    }

    /** A call that may throw. */
    @FunctionalInterface
    private interface Call {
        Object run() throws Exception;
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

    private static synchronized ClassLoader loader() throws IOException {
        if (loader == null) {
            List<URL> urls = new ArrayList<>();
            urls.add(url(MODULE.resolve("test-classes")));
            try (Stream<Path> jars = Files.list(MODULE.resolve("fixture-libs"))) {
                for (Path jar : jars.sorted().toList()) {
                    urls.add(url(jar));
                }
            }
            loader = new ChildFirst(urls.toArray(new URL[0]), HiveTestTables.class.getClassLoader());
        }
        return loader;
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
