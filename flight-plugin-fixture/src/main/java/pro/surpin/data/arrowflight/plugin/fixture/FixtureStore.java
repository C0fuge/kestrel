package pro.surpin.data.arrowflight.plugin.fixture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.StoreContext;

/**
 * A store with no files whose {@link #directories(String)} reports what the plugin sees: the
 * versions of its libraries, its context class loader and which server classes it can load.
 */
final class FixtureStore implements DataStore {

    private final StoreContext context;
    private final String root;

    /**
     * Creates the store.
     *
     * @param context server context
     * @param root root URI
     */
    FixtureStore(StoreContext context, String root) {
        this.context = context;
        this.root = root;
    }

    @Override
    public String kind() {
        return "fixture";
    }

    @Override
    public boolean shared() {
        return true;
    }

    @Override
    public boolean hasLocality() {
        return false;
    }

    @Override
    public String root() {
        return root;
    }

    @Override
    public boolean exists(String relativePath) {
        return relativePath == null || relativePath.isEmpty();
    }

    @Override
    public Map<String, String> directories(String relativePath) throws IOException {
        ClassLoader own = FixtureStore.class.getClassLoader();
        Map<String, String> report = new LinkedHashMap<>();
        report.put("jackson", com.fasterxml.jackson.databind.cfg.PackageVersion.VERSION.toString());
        report.put("jacksonWorks", new ObjectMapper().writeValueAsString(Map.of("a", 1)));
        report.put("guava", String.valueOf(Preconditions.class.getProtectionDomain()
                .getCodeSource().getLocation()));
        report.put("tcclIsPlugin", String.valueOf(Thread.currentThread().getContextClassLoader() == own));
        report.put("seesServer", String.valueOf(canLoad("pro.surpin.data.arrowflight.server.LogUtil")));
        report.put("seesHadoop", String.valueOf(canLoad("org.apache.hadoop.conf.Configuration")));
        report.put("seesDuckDb", String.valueOf(canLoad("org.duckdb.DuckDBDriver")));
        report.put("spiFromServer", String.valueOf(DataStore.class.getClassLoader() != own));
        report.put("slf4jFromServer", String.valueOf(
                org.slf4j.LoggerFactory.class.getClassLoader() != own));
        report.put("greeting", context.config().get("greeting", "?"));
        AtomicReference<ClassLoader> threadLoader = new AtomicReference<>();
        Thread thread = context.threadFactory("fixture-worker")
                .newThread(() -> threadLoader.set(Thread.currentThread().getContextClassLoader()));
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        report.put("threadTcclIsPlugin", String.valueOf(threadLoader.get() == own));
        report.put("threadDaemon", String.valueOf(thread.isDaemon()));
        return report;
    }

    @Override
    public List<DataFile> parquetFiles(String relativeDirectory) {
        return List.of();
    }

    @Override
    public DataFile file(String relativePath) throws IOException {
        throw new FileNotFoundException(relativePath);
    }

    @Override
    public ReadableFile open(String relativePath) throws IOException {
        throw new FileNotFoundException(relativePath);
    }

    @Override
    public ReadableFile open(DataFile file) {
        throw new IllegalArgumentException("No files: " + file.relativePath());
    }

    @Override
    public String duckDbUri(String relativePath) {
        return root + relativePath;
    }

    @Override
    public List<String> duckDbRoots() {
        return List.of(root);
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        setup.refuseFunctions("fixture_refused");
    }

    @Override
    public Map<String, Long> localInventory(String host) {
        return Map.of();
    }

    /**
     * Tells whether a class is visible to this plugin.
     *
     * @param name class name
     * @return true when it loads
     */
    private static boolean canLoad(String name) {
        try {
            Class.forName(name, false, FixtureStore.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }
}
