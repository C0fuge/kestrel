package pro.surpin.data.arrowflight.plugin.lakefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pro.surpin.data.arrowflight.plugin.s3.S3DataStore;
import pro.surpin.data.arrowflight.plugin.s3.S3Settings;
import pro.surpin.data.arrowflight.plugin.s3.S3Uri;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.storage.DataFile;
import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A lakeFS data directory read through the lakeFS S3 gateway.
 *
 * <p>When the configured ref is a branch and pinning is on, every listing first resolves the
 * branch to its head commit (one REST call, shared by all listings within {@code pinTtlMillis})
 * and lists that commit instead. The files it returns carry the commit in their relative path,
 * {@code @<commit>/<path>}: that is the name the planner puts into tickets, so every node and
 * endpoint of one query reads the same commit, whatever the branch does meanwhile. Commit ids
 * and tags, and branches with pinning off, are read as they are.
 */
public final class LakeFsDataStore implements DataStore {

    /** Starts a relative path pinned to a commit: {@code @<commit>/<path>}. */
    static final String PIN = "@";

    private static final Logger LOGGER = LoggerFactory.getLogger(LakeFsDataStore.class);

    /** Commits whose stores are kept; older ones are closed (their files stay readable). */
    private static final int MAX_STORES = 4;

    private final LakeFsUri root;
    private final S3Settings settings;
    private final S3Client client;
    private final RefResolver resolver;
    private final boolean pinBranches;
    private final long pinTtlNanos;
    private final LongSupplier nanoClock;
    private final ThreadFactory threads;
    private final String node;
    private final Map<String, S3DataStore> stores = new LinkedHashMap<>(8, 0.75f, true);
    private final Object resolution = new Object();
    private Resolved resolved;

    /**
     * The ref a listing reads.
     *
     * @param ref commit id, or the configured ref when it is not a pinned branch
     * @param pinned whether relative paths carry the ref
     * @param atNanos when it was resolved
     */
    private record Resolved(String ref, boolean pinned, long atNanos) {
    }

    /**
     * Creates a store.
     *
     * @param root data directory
     * @param settings S3 gateway settings with credentials
     * @param client S3 client of the gateway; this store closes it
     * @param resolver branch to commit resolution
     * @param pinBranches whether branches are pinned to their head commit per query plan
     * @param pinTtlMillis how long one resolution is shared by listings
     * @param nanoClock monotonic clock in nanoseconds
     * @param threads creates the listing threads
     * @param node name of this server node, for logs
     */
    public LakeFsDataStore(LakeFsUri root, S3Settings settings, S3Client client,
            RefResolver resolver, boolean pinBranches, long pinTtlMillis, LongSupplier nanoClock,
            ThreadFactory threads, String node) {
        this.root = Objects.requireNonNull(root, "root");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.client = Objects.requireNonNull(client, "client");
        this.resolver = Objects.requireNonNull(resolver, "resolver");
        this.pinBranches = pinBranches;
        this.pinTtlNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, pinTtlMillis));
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.threads = Objects.requireNonNull(threads, "threads");
        this.node = Objects.requireNonNull(node, "node");
    }

    @Override
    public String kind() {
        return "lakefs";
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
        return root.uri();
    }

    @Override
    public boolean exists(String relativePath) throws IOException {
        Target target = target(relativePath);
        return target.store().exists(target.path());
    }

    @Override
    public Map<String, String> directories(String relativePath) throws IOException {
        Target target = target(relativePath);
        return target.store().directories(target.path());
    }

    @Override
    public List<DataFile> parquetFiles(String relativeDirectory) throws IOException {
        Target target = target(relativeDirectory);
        return pinned(target, target.store().parquetFiles(target.path()));
    }

    @Override
    public List<DataFile> tableFiles(String schema, String table) throws IOException {
        Target target = target(null);
        String directory = schema + "/" + table;
        return target.store().exists(directory)
                ? pinned(target, target.store().parquetFiles(directory)) : List.of();
    }

    @Override
    public DataFile file(String relativePath) throws IOException {
        Target target = target(relativePath);
        DataFile file = target.store().file(target.path());
        return target.pinnedRef() == null ? file : pin(target.pinnedRef(), file);
    }

    @Override
    public ReadableFile open(String relativePath) throws IOException {
        return open(file(relativePath));
    }

    @Override
    public ReadableFile open(DataFile file) {
        Target target = targetOf(file.relativePath());
        DataFile own = new DataFile(target.path(), file.location(), file.size(),
                file.modificationTime(), file.hosts());
        return target.store().open(own);
    }

    @Override
    public String duckDbUri(String relativePath) {
        Target target = targetOf(relativePath);
        return target.store().duckDbUri(target.path());
    }

    @Override
    public String duckDbUri(DataFile file) {
        return file.location();
    }

    @Override
    public List<String> duckDbRoots() {
        // Pinned reads name the commit, not the branch: DuckDB may read the whole repository.
        return List.of(repositoryRoot().root());
    }

    @Override
    public void contributeDuckDb(DuckDbSetup setup) {
        S3DataStore.contribute(setup, settings, repositoryRoot());
    }

    @Override
    public Map<String, Long> localInventory(String host) {
        return Map.of();
    }

    @Override
    public void close() {
        synchronized (stores) {
            for (S3DataStore store : stores.values()) {
                store.close();
            }
            stores.clear();
        }
        client.close();
    }

    private S3Uri repositoryRoot() {
        return S3Uri.parse("s3://" + root.repository());
    }

    /**
     * A relative path on the store of one ref.
     *
     * @param store store of the ref
     * @param path path below the data directory at that ref
     * @param pinnedRef commit the path is pinned to, null when unpinned
     */
    private record Target(S3DataStore store, String path, String pinnedRef) {
    }

    /**
     * Finds where a listing or lookup reads: a pinned path names its commit, any other path
     * reads the current resolution of the configured ref.
     */
    private Target target(String relativePath) throws IOException {
        String path = relativePath == null ? "" : relativePath;
        if (path.startsWith(PIN)) {
            return targetOf(path);
        }
        Resolved current = resolve();
        return new Target(store(current.ref()), path, current.pinned() ? current.ref() : null);
    }

    /**
     * Finds where a path reads without asking lakeFS: a pinned path names its commit, an
     * unpinned one reads the configured ref (or the last resolution of it).
     */
    private Target targetOf(String relativePath) {
        String path = relativePath == null ? "" : relativePath;
        if (path.startsWith(PIN)) {
            int slash = path.indexOf('/');
            String ref = LakeFsUri.requireRef(slash < 0 ? path.substring(1) : path.substring(1, slash));
            return new Target(store(ref), slash < 0 ? "" : path.substring(slash + 1), ref);
        }
        Resolved last;
        synchronized (resolution) {
            last = resolved;
        }
        String ref = last == null || last.pinned() ? root.ref() : last.ref();
        return new Target(store(ref), path, null);
    }

    /**
     * Resolves the configured ref, sharing one resolution among the listings of a query plan.
     *
     * @return ref to read
     * @throws IOException when lakeFS cannot resolve the branch
     */
    private Resolved resolve() throws IOException {
        if (!pinBranches) {
            return new Resolved(root.ref(), false, 0L);
        }
        synchronized (resolution) {
            long now = nanoClock.getAsLong();
            if (resolved != null && now - resolved.atNanos() < pinTtlNanos) {
                return resolved;
            }
            Resolved next = resolver.branchHead(root.repository(), root.ref())
                    .map(commit -> new Resolved(LakeFsUri.requireRef(commit), true, now))
                    .orElseGet(() -> new Resolved(root.ref(), false, now));
            if (resolved == null || !resolved.ref().equals(next.ref())) {
                LOGGER.info("node={} lakefs=refResolved repository={} ref={} reads={} pinned={}",
                        node, root.repository(), root.ref(), next.ref(), next.pinned());
            }
            resolved = next;
            return next;
        }
    }

    private S3DataStore store(String ref) {
        synchronized (stores) {
            S3DataStore store = stores.get(ref);
            if (store == null) {
                store = new S3DataStore(root.at(ref).s3Uri(), settings, client, nanoClock, threads,
                        node, false);
                stores.put(ref, store);
                if (stores.size() > MAX_STORES) {
                    var eldest = stores.entrySet().iterator().next();
                    if (!eldest.getKey().equals(ref)) {
                        // Shares the client: reads already handed out keep working.
                        eldest.getValue().close();
                        stores.remove(eldest.getKey());
                    }
                }
            }
            return store;
        }
    }

    private static List<DataFile> pinned(Target target, List<DataFile> files) {
        if (target.pinnedRef() == null) {
            return files;
        }
        List<DataFile> result = new ArrayList<>(files.size());
        for (DataFile file : files) {
            result.add(pin(target.pinnedRef(), file));
        }
        return result;
    }

    private static DataFile pin(String commit, DataFile file) {
        return new DataFile(PIN + commit + "/" + file.relativePath(), file.location(), file.size(),
                file.modificationTime(), file.hosts());
    }
}
