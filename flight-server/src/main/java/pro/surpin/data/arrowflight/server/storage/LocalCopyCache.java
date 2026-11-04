package pro.surpin.data.arrowflight.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.spi.storage.DataFile;

/**
 * Bounded node-local cache of whole copies of files DuckDB cannot read where they are (HDFS
 * DataNodes that require SASL data-transfer protection, where DuckDB reads short-circuit only).
 * A copy is keyed by location, length and modification time, so a rewritten file is copied
 * again; copies are reused across queries and evicted least recently used first. A copy in use
 * by a query is never evicted; when the copies in use exceed the limit, the extra copies are
 * deleted as soon as their last query releases them. A query's new copies are made only when the
 * disk of the cache directory keeps {@code minFreeBytes} free after them (unused copies are
 * evicted for room first); otherwise the query is refused naming the files, before anything is
 * copied, instead of filling the disk.
 */
public final class LocalCopyCache implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalCopyCache.class);
    private static final int BUFFER_BYTES = 1 << 20;

    /** Copies one file's bytes; the store's Java client in the server. */
    @FunctionalInterface
    public interface Copier {
        /**
         * Opens a file for reading from its start.
         *
         * @param file file to read
         * @return stream of the file's bytes
         * @throws IOException when the file cannot be opened
         */
        InputStream open(DataFile file) throws IOException;
    }

    /** Tells the free bytes of the disk of a directory; the file store's usable space in the server. */
    @FunctionalInterface
    public interface FreeSpace {
        /**
         * Returns the bytes that can still be written below a directory.
         *
         * @param directory existing directory
         * @return usable bytes
         * @throws IOException when the disk cannot be queried
         */
        long usableBytes(Path directory) throws IOException;
    }

    /** Free space of the file store that holds a directory. */
    public static final FreeSpace FILE_STORE = d -> Files.getFileStore(d).getUsableSpace();

    /**
     * Thrown when the disk of the cache directory has no room for a query's copies: a resource
     * limit of the node (clients get RESOURCE_EXHAUSTED and may retry), not a server fault.
     */
    public static final class NoRoomException extends IOException {
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message what did not fit and why
         */
        public NoRoomException(String message) {
            super(message);
        }

        /**
         * Finds a refusal for lack of copy room in a failure or its causes.
         *
         * @param failure failure, or null
         * @return the refusal, or null when there is none
         */
        public static NoRoomException in(Throwable failure) {
            for (Throwable t = failure; t != null; t = t.getCause()) {
                if (t instanceof NoRoomException noRoom) {
                    return noRoom;
                }
                if (t.getCause() == t) {
                    break;
                }
            }
            return null;
        }
    }

    /**
     * One cached copy.
     */
    private static final class Entry {
        private final String key;
        private final Path path;
        private final long bytes;
        private final CompletableFuture<Path> ready = new CompletableFuture<>();
        private int pins;

        /**
         * Creates an entry for a copy being made.
         *
         * @param key cache key
         * @param path local file of the copy
         * @param bytes file length
         */
        Entry(String key, Path path, long bytes) {
            this.key = key;
            this.path = path;
            this.bytes = bytes;
        }
    }

    private final Path directory;
    private final long maxBytes;
    private final long minFreeBytes;
    private final FreeSpace freeSpace;
    private final Copier copier;
    private final ExecutorService copyPool;
    /** Entries in least recently used order; guarded by this. */
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(64, 0.75f, true);
    /** Bytes of every entry, made or being made; guarded by this. */
    private long cachedBytes;
    /** Bytes of the entries being made, not yet complete on disk; guarded by this. */
    private long pendingBytes;
    /** Query that was last warned about the limit (evictFor runs once per copy batch); guarded by this. */
    private String overLimitQid;

    /**
     * Creates a cache.
     *
     * @param directory directory of the copies, created when missing
     * @param maxBytes bytes the copies not in use may take together
     * @param threads files copied at the same time
     * @param copier reads a file's bytes
     * @throws IOException when the directory cannot be created
     */
    public LocalCopyCache(Path directory, long maxBytes, int threads, Copier copier)
            throws IOException {
        this(directory, maxBytes, threads, copier, 0, FILE_STORE);
    }

    /**
     * Creates a cache that keeps bytes free on the disk of its directory.
     *
     * @param directory directory of the copies, created when missing
     * @param maxBytes bytes the copies not in use may take together
     * @param threads files copied at the same time
     * @param copier reads a file's bytes
     * @param minFreeBytes bytes that must stay free on the disk after a query's copies
     * @param freeSpace tells the free bytes of the disk
     * @throws IOException when the directory cannot be created
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public LocalCopyCache(Path directory, long maxBytes, int threads, Copier copier,
            long minFreeBytes, FreeSpace freeSpace) throws IOException {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
        }
        if (threads <= 0) {
            throw new IllegalArgumentException("threads must be positive: " + threads);
        }
        if (minFreeBytes < 0) {
            throw new IllegalArgumentException("minFreeBytes must not be negative: " + minFreeBytes);
        }
        this.minFreeBytes = minFreeBytes;
        this.freeSpace = Objects.requireNonNull(freeSpace, "freeSpace");
        this.directory = Files.createDirectories(Objects.requireNonNull(directory, "directory"));
        this.maxBytes = maxBytes;
        this.copier = Objects.requireNonNull(copier, "copier");
        AtomicInteger ids = new AtomicInteger();
        this.copyPool = Executors.newFixedThreadPool(threads, r -> {
            Thread thread = new Thread(r, "hdfs-local-copy-" + ids.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Returns the cache key of a file.
     *
     * @param file listed file
     * @return key that changes when the file is rewritten
     */
    static String key(DataFile file) {
        return file.location() + "\u0000" + file.size() + "\u0000" + file.modificationTime();
    }

    /**
     * Returns where the copy of a file goes: below a directory named by a hash of its key, at
     * the file's relative path, so that Hive partition directories ({@code k=v}) stay in the
     * path DuckDB reads.
     *
     * @param key cache key
     * @param relativePath path below the data directory
     * @return path relative to the cache directory
     */
    static String copyPath(String key, String relativePath) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(key.getBytes(StandardCharsets.UTF_8));
            String plain = relativePath.startsWith("/") ? relativePath.substring(1) : relativePath;
            if (plain.isEmpty() || plain.contains("..")) {
                plain = "file";
            }
            return HexFormat.of().formatHex(digest, 0, 16) + "/" + plain;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is missing", e);
        }
    }

    /**
     * Makes local copies of files for one query, copying those not cached in parallel; the
     * copies stay until the lease is closed.
     *
     * @param files files to copy
     * @return lease mapping each file's relative path to its local copy
     * @throws IOException when a file cannot be copied, or the disk has no room for the new
     *         copies (nothing is copied then); no copy stays pinned then
     */
    public Lease acquire(List<DataFile> files) throws IOException {
        // Not LogUtil.mark(): that is -1 unless timing is logged, and this time is always logged.
        long t = System.nanoTime();
        Lease lease = new Lease();
        List<Entry> made = new ArrayList<>();
        try {
            List<CompletableFuture<Path>> waits = new ArrayList<>(files.size());
            synchronized (this) {
                // The new copies are known, and must have room, before any of them is started.
                List<DataFile> toCopy = new ArrayList<>();
                Set<String> paths = new HashSet<>();
                Set<String> keys = new HashSet<>();
                long needed = 0;
                for (DataFile file : files) {
                    String key = key(file);
                    if (paths.add(file.relativePath()) && !entries.containsKey(key) && keys.add(key)) {
                        toCopy.add(file);
                        needed += file.size();
                    }
                }
                if (!toCopy.isEmpty()) {
                    evictFor(needed);
                    requireRoom(needed, toCopy);
                }
                for (DataFile file : files) {
                    String key = key(file);
                    if (lease.entries.containsKey(file.relativePath())) {
                        continue;
                    }
                    Entry entry = entries.get(key);
                    if (entry == null) {
                        entry = new Entry(key, directory.resolve(copyPath(key, file.relativePath())),
                                file.size());
                        entries.put(key, entry);
                        cachedBytes += entry.bytes;
                        pendingBytes += entry.bytes;
                        made.add(entry);
                        Entry copy = entry;
                        copyPool.execute(() -> copy(file, copy));
                    } else {
                        lease.reused++;
                    }
                    entry.pins++;
                    lease.entries.put(file.relativePath(), entry);
                    waits.add(entry.ready);
                }
            }
            for (CompletableFuture<Path> wait : waits) {
                wait.get();
            }
        } catch (ExecutionException e) {
            lease.close();
            Throwable cause = e.getCause();
            throw cause instanceof IOException io ? io : new IOException(cause);
        } catch (InterruptedException e) {
            lease.close();
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while copying files to " + directory, e);
        } catch (RuntimeException e) {
            lease.close();
            throw e;
        }
        for (Entry entry : made) {
            lease.copiedBytes += entry.bytes;
        }
        lease.copied = made.size();
        lease.millis = (System.nanoTime() - t) / 1_000_000L;
        return lease;
    }

    /**
     * Copies one file into its entry's local file and completes the entry.
     *
     * @param file file to copy
     * @param entry entry being made
     */
    private void copy(DataFile file, Entry entry) {
        long started = System.nanoTime();
        Path partial = entry.path.resolveSibling(entry.path.getFileName() + ".part");
        try {
            long copied;
            Path parent = partial.getParent();
            if (parent != null) {
                // Always below the cache directory; the check is for SpotBugs.
                Files.createDirectories(parent);
            }
            try (InputStream in = copier.open(file);
                    OutputStream out = Files.newOutputStream(partial)) {
                copied = transfer(in, out);
            }
            if (copied != file.size()) {
                throw new IOException("Copied " + copied + " bytes of " + file.location()
                        + ", which has " + file.size() + ": it changed while it was copied");
            }
            Files.move(partial, entry.path, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            long ms = Math.max(1L, (System.nanoTime() - started) / 1_000_000L);
            LOGGER.info("node={} hdfs=localCopy file={} bytes={} ms={} MBps={}", LogUtil.node(),
                    file.relativePath(), copied, ms, copied * 1000L / ms / (1024 * 1024));
            synchronized (this) {
                pendingBytes -= entry.bytes;
            }
            entry.ready.complete(entry.path);
        } catch (IOException | RuntimeException e) {
            deleteCopy(partial);
            synchronized (this) {
                pendingBytes -= entry.bytes;
                if (entries.get(entry.key) == entry) {
                    entries.remove(entry.key);
                    cachedBytes -= entry.bytes;
                }
            }
            LOGGER.warn("node={} hdfs=localCopyFailed file={} reason='{}'", LogUtil.node(),
                    file.relativePath(), e.toString());
            entry.ready.completeExceptionally(new IOException("Cannot copy " + file.relativePath()
                    + " to the node-local cache " + directory + ": " + e.getMessage(), e));
        }
    }

    /**
     * Copies a stream to another.
     *
     * @param in source
     * @param out target
     * @return bytes copied
     * @throws IOException on read or write failure
     */
    private static long transfer(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[BUFFER_BYTES];
        long total = 0;
        int n;
        while ((n = in.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
            total += n;
        }
        return total;
    }

    /**
     * Evicts unused copies, least recently used first, until a new copy fits the limit or no
     * unused copy is left; the caller holds this.
     *
     * @param bytes length of the new copy
     */
    private void evictFor(long bytes) {
        Iterator<Entry> it = entries.values().iterator();
        while (cachedBytes + bytes > maxBytes && it.hasNext()) {
            Entry entry = it.next();
            if (entry.pins == 0 && entry.ready.isDone()) {
                it.remove();
                cachedBytes -= entry.bytes;
                deleteCopy(entry.path);
            }
        }
        if (cachedBytes + bytes > maxBytes) {
            // A query copying in batches comes here once per batch: warn once per query.
            String qid = LogUtil.qid();
            if (qid == null || !qid.equals(overLimitQid)) {
                overLimitQid = qid;
                LOGGER.warn("qid={} node={} hdfs=localCopyOverLimit cachedBytes={} newBytes={}"
                        + " maxBytes={}: copies in use are kept, the extra ones are deleted after"
                        + " use (logged once per query)", qid, LogUtil.node(), cachedBytes, bytes,
                        maxBytes);
            } else {
                LOGGER.debug("qid={} node={} hdfs=localCopyOverLimit cachedBytes={} newBytes={}"
                        + " maxBytes={}", qid, LogUtil.node(), cachedBytes, bytes, maxBytes);
            }
        }
    }

    /**
     * Makes sure the disk keeps {@code minFreeBytes} free after a query's new copies, evicting
     * unused copies (also below the limit) for room; the caller holds this. Copies being made
     * count with their full length, so the check is conservative while they are written.
     *
     * @param needed bytes of the new copies
     * @param toCopy files of the new copies, for the message
     * @throws IOException when the disk has no room for them even without unused copies
     */
    private void requireRoom(long needed, List<DataFile> toCopy) throws IOException {
        long free = freeSpace.usableBytes(directory) - pendingBytes;
        Iterator<Entry> it = entries.values().iterator();
        while (free - needed < minFreeBytes && it.hasNext()) {
            Entry entry = it.next();
            if (entry.pins == 0 && entry.ready.isDone()) {
                it.remove();
                cachedBytes -= entry.bytes;
                deleteCopy(entry.path);
                free = freeSpace.usableBytes(directory) - pendingBytes;
            }
        }
        if (free - needed >= minFreeBytes) {
            return;
        }
        long inUse = cachedBytes;
        LOGGER.warn("node={} hdfs=localCopyNoSpace dir={} files={} neededBytes={} freeBytes={}"
                + " minFreeBytes={} inUseBytes={}", LogUtil.node(), directory, toCopy.size(), needed,
                free, minFreeBytes, inUse);
        throw new NoRoomException("No room on the disk of the node-local copy cache " + directory
                + " for this query's " + toCopy.size() + " file copies (" + needed + " bytes, e.g. "
                + toCopy.get(0).relativePath() + "): " + Math.max(0, free) + " bytes are free and "
                + minFreeBytes + " must stay free (hdfsLocalCopyMinFreeBytes); copies in use by"
                + " running queries take " + inUse + " bytes. Nothing was copied; retry when other"
                + " queries end, or give hdfsLocalCopyDir a larger disk");
    }

    /**
     * Returns the bytes one query's new copies may take now: the cache limit, or less when the
     * disk of the cache directory has less room (keeping {@code minFreeBytes} free, counting
     * unused copies as room since they can be evicted).
     *
     * @return bytes, at least 1
     * @throws IOException when the disk cannot be queried
     */
    public synchronized long room() throws IOException {
        long evictable = 0;
        for (Entry entry : entries.values()) {
            if (entry.pins == 0 && entry.ready.isDone()) {
                evictable += entry.bytes;
            }
        }
        long disk = freeSpace.usableBytes(directory) - pendingBytes + evictable - minFreeBytes;
        return Math.max(1L, Math.min(maxBytes, disk));
    }

    /**
     * Returns the bytes that must stay free on the disk of the cache directory.
     *
     * @return bytes
     */
    public long minFreeBytes() {
        return minFreeBytes;
    }

    /**
     * Releases one pin of an entry, deleting unused copies while the cache is over its limit.
     *
     * @param entry released entry
     */
    private synchronized void release(Entry entry) {
        entry.pins--;
        if (cachedBytes > maxBytes) {
            evictFor(0);
        }
    }

    /**
     * Returns the bytes the copies not in use may take together.
     *
     * @return the limit in bytes
     */
    public long maxBytes() {
        return maxBytes;
    }

    /**
     * Returns the bytes of the copies made or being made.
     *
     * @return bytes
     */
    public synchronized long cachedBytes() {
        return cachedBytes;
    }

    /**
     * Returns the number of copies made or being made.
     *
     * @return copies
     */
    public synchronized int size() {
        return entries.size();
    }

    /**
     * Returns the directory of the copies.
     *
     * @return directory
     */
    public Path directory() {
        return directory;
    }

    @Override
    public void close() {
        copyPool.shutdownNow();
        synchronized (this) {
            for (Entry entry : entries.values()) {
                deleteCopy(entry.path);
            }
            entries.clear();
            cachedBytes = 0;
        }
    }

    /**
     * Deletes a copy and the directories above it it leaves empty, logging a failure.
     *
     * @param path copy
     */
    private void deleteCopy(Path path) {
        try {
            Files.deleteIfExists(path);
            for (Path dir = path.getParent(); dir != null && dir.startsWith(directory)
                    && !dir.equals(directory); dir = dir.getParent()) {
                try (java.util.stream.Stream<Path> children = Files.list(dir)) {
                    if (children.findAny().isPresent()) {
                        break;
                    }
                }
                Files.deleteIfExists(dir);
            }
        } catch (IOException e) {
            LOGGER.warn("node={} hdfs=localCopyDeleteFailed path={} reason='{}'", LogUtil.node(),
                    path, e.toString());
        }
    }

    /**
     * Local copies held for one query.
     */
    public final class Lease implements AutoCloseable {
        private final Map<String, Entry> entries = new LinkedHashMap<>();
        private int copied;
        private int reused;
        private long copiedBytes;
        private long millis;
        private boolean closed;

        /**
         * Returns the local copy of a file.
         *
         * @param relativePath file path below the data directory
         * @return local path, or null when the lease holds no copy of it
         */
        public Path localPath(String relativePath) {
            Entry entry = entries.get(relativePath);
            return entry == null ? null : entry.path;
        }

        /**
         * Returns the number of files this lease copied.
         *
         * @return files copied for this query
         */
        public int copied() {
            return copied;
        }

        /**
         * Returns the number of files this lease found copied already.
         *
         * @return files reused
         */
        public int reused() {
            return reused;
        }

        /**
         * Returns the bytes this lease copied.
         *
         * @return bytes
         */
        public long copiedBytes() {
            return copiedBytes;
        }

        /**
         * Returns how long making the copies took.
         *
         * @return milliseconds
         */
        public long millis() {
            return millis;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            for (Entry entry : entries.values()) {
                release(entry);
            }
        }
    }
}
