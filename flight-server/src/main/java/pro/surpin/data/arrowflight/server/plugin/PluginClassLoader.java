package pro.surpin.data.arrowflight.server.plugin;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * Class loader of one plugin, child first: a plugin's own jars win over everything but the JDK,
 * the SPI and slf4j (always the server's), and with {@code share=hadoop} the cluster's
 * {@code org.apache.hadoop.*} classes (but not {@code org.apache.hadoop.hive.*}) and Hadoop's
 * {@code core-*.xml}/{@code hdfs-*.xml} resources, and with {@code share=arrow} the server's
 * {@code org.apache.arrow.*} classes (one Arrow memory and C data library per JVM, so Arrow data
 * crosses between plugin and server). Other server classes are invisible.
 */
public final class PluginClassLoader extends URLClassLoader {

    static {
        ClassLoader.registerAsParallelCapable();
    }

    /** Packages always taken from the server. */
    private static final List<String> ALWAYS_SHARED = List.of(
            "pro.surpin.data.arrowflight.spi.", "org.slf4j.");
    /** Hadoop packages a {@code share=hadoop} plugin takes from the server. */
    private static final String HADOOP = "org.apache.hadoop.";
    /** Hadoop packages a plugin always has its own copy of. */
    private static final String HIVE = "org.apache.hadoop.hive.";
    /** Arrow packages a {@code share=arrow} plugin takes from the server. */
    private static final String ARROW = "org.apache.arrow.";

    private final ClassLoader server;
    private final boolean shareHadoop;
    private final boolean shareArrow;

    /**
     * Creates a loader.
     *
     * @param name plugin name
     * @param jars plugin jars
     * @param server the server's class loader, for shared classes
     * @param shareHadoop whether Hadoop classes come from the server
     * @param shareArrow whether Arrow classes come from the server
     */
    public PluginClassLoader(String name, URL[] jars, ClassLoader server, boolean shareHadoop,
            boolean shareArrow) {
        super("plugin-" + name, jars, ClassLoader.getPlatformClassLoader());
        this.server = Objects.requireNonNull(server, "server");
        this.shareHadoop = shareHadoop;
        this.shareArrow = shareArrow;
    }

    /**
     * Tells whether a class comes from the server.
     *
     * @param className binary class name
     * @return true for the SPI, slf4j and (when shared) Hadoop and Arrow
     */
    boolean isShared(String className) {
        for (String prefix : ALWAYS_SHARED) {
            if (className.startsWith(prefix)) {
                return true;
            }
        }
        if (shareArrow && className.startsWith(ARROW)) {
            return true;
        }
        return shareHadoop && className.startsWith(HADOOP) && !className.startsWith(HIVE);
    }

    /**
     * Tells whether a resource comes from the server.
     *
     * @param resource resource name
     * @return true for resources of shared packages and, with Hadoop shared, its site files
     */
    boolean isSharedResource(String resource) {
        String asClass = resource.replace('/', '.');
        if (isShared(asClass)) {
            return true;
        }
        return shareHadoop && !resource.contains("/") && resource.endsWith(".xml")
                && (resource.startsWith("core-") || resource.startsWith("hdfs-"));
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                if (isShared(name)) {
                    loaded = server.loadClass(name);
                } else if (name.startsWith("java.")) {
                    loaded = getParent().loadClass(name);
                } else {
                    try {
                        loaded = findClass(name);
                    } catch (ClassNotFoundException e) {
                        loaded = getParent().loadClass(name);
                    }
                }
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    @Override
    public URL getResource(String name) {
        if (isSharedResource(name)) {
            return server.getResource(name);
        }
        URL own = findResource(name);
        return own != null ? own : getParent().getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        List<URL> result = new ArrayList<>();
        if (isSharedResource(name)) {
            result.addAll(Collections.list(server.getResources(name)));
        } else {
            result.addAll(Collections.list(findResources(name)));
            result.addAll(Collections.list(getParent().getResources(name)));
        }
        return Collections.enumeration(result);
    }
}
