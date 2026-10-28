package pro.surpin.data.arrowflight.plugin.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.zip.CRC32C;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Second tier of the S3 read cache (item 17 (a), opt-in): blocks read from S3 are also written to
 * files in a local directory, bounded in bytes, least recently used deleted first, and a block the
 * memory tier no longer holds is read from its file instead of S3. Blocks are written by one
 * background thread from a short queue; when the queue is full a block is not written (it stays a
 * cache). Each file's CRC-32C is kept in memory and checked on every read: a file that does not
 * match, is gone or cannot be read is a miss, never an answer. The index lives in memory only, so
 * the directory starts empty: files of an earlier run are deleted at the start and all files at
 * close. A lock file keeps two servers from using one directory.
 *
 * <p>With admission on reuse, a block is written only when it was offered before and is still
 * among the last {@code capacity / block size} names offered and not written.
 */
final class S3DiskCache implements AutoCloseable {

    /**
     * Where the disk tier lives and how large it is.
     *
     * @param path directory; created when missing
     * @param capacityBytes most bytes of blocks held
     */
    record Settings(Path path, long capacityBytes) {

        /**
         * Checks the values.
         *
         * @param path directory
         * @param capacityBytes most bytes held
         */
        Settings {
            Objects.requireNonNull(path, "path");
            if (capacityBytes <= 0) {
                throw new IllegalArgumentException("disk cache capacity must be positive: " + capacityBytes);
            }
        }
    }

    /**
     * The disk tier's use at one moment.
     *
     * @param capacityBytes most bytes held
     * @param usedBytes bytes of all block files
     * @param blocks block files
     * @param hits blocks read from a file
     * @param misses blocks looked up and not found (or not readable)
     * @param writes blocks written
     * @param evictions block files deleted to make room
     * @param dropped blocks not written because the write queue was full
     * @param errors files that failed to write, read or match their checksum
     * @param notAdmitted blocks offered and not written (admission on reuse)
     */
    record Use(long capacityBytes, long usedBytes, int blocks, long hits, long misses, long writes, long evictions,
            long dropped, long errors, long notAdmitted) {
    }

    /** Most blocks waiting to be written; bounds the heap the writes hold. */
    static final int MAX_PENDING = 64;
    private static final Logger LOGGER = LoggerFactory.getLogger(S3DiskCache.class);
    private static final Pattern BLOCK_FILE = Pattern.compile("[0-9]+\\.(blk|tmp)");
    private static final Pattern BLOCK_DIR = Pattern.compile("[0-9a-f]{2}");

    private record Entry(Path file, int length, int crc) {
    }

    private record Pending(S3BlockCache.BlockKey key, S3BlockCache.Block block) {
    }

    private final Settings settings;
    private final String kind;
    private final int ghostEntries;
    private final Path blocksDir;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final BlockingQueue<Pending> pending = new ArrayBlockingQueue<>(MAX_PENDING);
    private final Thread writer;
    private final LinkedHashMap<S3BlockCache.BlockKey, Entry> index = new LinkedHashMap<>(256, 0.75f, true);
    private final LinkedHashMap<S3BlockCache.BlockKey, Boolean> ghosts = new LinkedHashMap<>(256, 0.75f, false);
    private long nextFile;
    private long usedBytes;
    private long hits;
    private long misses;
    private long writes;
    private long evictions;
    private long dropped;
    private long errors;
    private long notAdmitted;
    private boolean closed;
    /** Blocks queued and not yet written or given up. */
    private final AtomicInteger outstanding = new AtomicInteger();

    /**
     * Opens the disk tier: takes the directory's lock, deletes block files of an earlier run and
     * starts the writer.
     *
     * @param settings directory and size
     * @param kind plugin name ({@code s3}, {@code lakefs}): names the lock file and block directory
     * @param admitOnReuse write a block only on its second offer within the tier's reach
     * @throws IOException when the directory cannot be used or another server holds its lock
     */
    S3DiskCache(Settings settings, String kind, boolean admitOnReuse) throws IOException {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.kind = Objects.requireNonNull(kind, "kind");
        this.ghostEntries = admitOnReuse
                ? (int) Math.max(1, Math.min(Integer.MAX_VALUE, settings.capacityBytes() / S3CacheEndpoint.BLOCK_BYTES))
                : 0;
        Path root = settings.path();
        Files.createDirectories(root);
        this.lockChannel = FileChannel.open(root.resolve(kind + "-cache.lock"), StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        FileLock taken;
        try {
            taken = lockChannel.tryLock();
        } catch (OverlappingFileLockException e) {
            taken = null;
        }
        if (taken == null) {
            lockChannel.close();
            throw new IOException("the disk cache directory " + root + " is in use by another " + kind
                    + " read cache (lock " + kind + "-cache.lock)");
        }
        this.lock = taken;
        try {
            this.blocksDir = root.resolve(kind + "-cache");
            Files.createDirectories(blocksDir);
            ownerOnly(blocksDir);
            deleteBlockFiles(blocksDir);
            long usable = Files.getFileStore(blocksDir).getUsableSpace();
            if (usable < settings.capacityBytes()) {
                LOGGER.warn("{}=cacheDiskSmall path={} capacityBytes={} usableBytes={} (blocks that do not fit"
                        + " are not written)", kind, root, settings.capacityBytes(), usable);
            }
        } catch (IOException | RuntimeException e) {
            releaseLock();
            throw e;
        }
        this.writer = new Thread(this::writeLoop, kind + "-cache-disk");
        writer.setDaemon(true);
        writer.start();
    }

    /**
     * Returns the directory and size.
     *
     * @return settings
     */
    Settings settings() {
        return settings;
    }

    /**
     * Reads a block from its file.
     *
     * @param key block name
     * @return the block, or null when it has no file or the file is gone, short or damaged
     */
    S3BlockCache.Block get(S3BlockCache.BlockKey key) {
        Entry entry;
        synchronized (this) {
            entry = index.get(key);
            if (entry == null) {
                misses++;
                return null;
            }
        }
        try (CheckedInputStream in = new CheckedInputStream(Files.newInputStream(entry.file()), new CRC32C())) {
            S3BlockCache.Block block = S3BlockCache.Block.read(in, entry.length());
            if (in.read() < 0 && (int) in.getChecksum().getValue() == entry.crc()) {
                synchronized (this) {
                    hits++;
                }
                return block;
            }
            LOGGER.warn("{}=cacheDiskDamaged file={} (read from S3 instead)", kind, entry.file());
        } catch (NoSuchFileException e) {
            // Deleted between the lookup and the read (evicted, or by hand): a miss.
            forget(key, entry, false);
            return null;
        } catch (IOException e) {
            LOGGER.warn("{}=cacheDiskReadFailed file={} error={} (read from S3 instead)", kind, entry.file(),
                    e.toString());
        }
        forget(key, entry, true);
        return null;
    }

    /**
     * Offers a block read from S3 for writing; it is queued, or not written when admission on
     * reuse holds it back, the queue is full, it is held already or it is larger than the tier.
     *
     * @param key block name
     * @param block the block
     */
    void offer(S3BlockCache.BlockKey key, S3BlockCache.Block block) {
        synchronized (this) {
            if (closed || block.length() > settings.capacityBytes() || index.containsKey(key)) {
                return;
            }
            if (ghostEntries > 0 && ghosts.remove(key) == null) {
                ghosts.put(key, Boolean.TRUE);
                if (ghosts.size() > ghostEntries) {
                    Iterator<S3BlockCache.BlockKey> eldest = ghosts.keySet().iterator();
                    eldest.next();
                    eldest.remove();
                }
                notAdmitted++;
                return;
            }
        }
        outstanding.incrementAndGet();
        if (!pending.offer(new Pending(key, block))) {
            outstanding.decrementAndGet();
            synchronized (this) {
                dropped++;
            }
        }
    }

    /**
     * Waits until every block queued so far is written; for tests.
     *
     * @param timeoutMillis most milliseconds to wait
     * @return whether the queue emptied in time
     * @throws InterruptedException when interrupted while waiting
     */
    boolean awaitWrites(long timeoutMillis) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < end) {
            if (outstanding.get() == 0) {
                return true;
            }
            Thread.sleep(5);
        }
        return false;
    }

    /** Writes queued blocks until closed. */
    private void writeLoop() {
        while (true) {
            Pending next;
            try {
                next = pending.take();
            } catch (InterruptedException e) {
                return;
            }
            try {
                write(next.key(), next.block());
            } finally {
                outstanding.decrementAndGet();
            }
        }
    }

    /**
     * Writes one block to a new file and adds it to the index, deleting the least recently used
     * files beyond the capacity.
     *
     * @param key block name
     * @param block the block
     */
    private void write(S3BlockCache.BlockKey key, S3BlockCache.Block block) {
        long number;
        synchronized (this) {
            if (index.containsKey(key)) {
                return;
            }
            number = nextFile++;
        }
        Path dir = blocksDir.resolve(String.format("%02x", number & 0xff));
        Path tmp = dir.resolve(number + ".tmp");
        Path file = dir.resolve(number + ".blk");
        CRC32C crc = new CRC32C();
        try {
            Files.createDirectories(dir);
            try (OutputStream out = new CheckedOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), 64 << 10), crc)) {
                block.write(out, 0, block.length());
            }
            Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            deleteQuietly(tmp);
            synchronized (this) {
                errors++;
            }
            LOGGER.debug("{}=cacheDiskWriteFailed file={} error={}", kind, file, e.toString());
            return;
        }
        List<Path> evicted = new ArrayList<>();
        synchronized (this) {
            if (closed) {
                evicted.add(file);
            } else {
                index.put(key, new Entry(file, block.length(), (int) crc.getValue()));
                usedBytes += block.length();
                writes++;
                Iterator<Map.Entry<S3BlockCache.BlockKey, Entry>> eldest = index.entrySet().iterator();
                while (usedBytes > settings.capacityBytes() && eldest.hasNext()) {
                    Entry e = eldest.next().getValue();
                    usedBytes -= e.length();
                    evicted.add(e.file());
                    eldest.remove();
                    evictions++;
                }
            }
        }
        evicted.forEach(S3DiskCache::deleteQuietly);
    }

    /**
     * Drops an index entry whose file could not be used.
     *
     * @param key block name
     * @param entry entry looked up
     * @param error whether to count an error
     */
    private void forget(S3BlockCache.BlockKey key, Entry entry, boolean error) {
        boolean removed;
        synchronized (this) {
            removed = index.remove(key, entry);
            if (removed) {
                usedBytes -= entry.length();
            }
            misses++;
            if (error) {
                errors++;
            }
        }
        if (removed) {
            deleteQuietly(entry.file());
        }
    }

    /**
     * Returns the tier's use so far.
     *
     * @return counts taken together
     */
    synchronized Use use() {
        return new Use(settings.capacityBytes(), usedBytes, index.size(), hits, misses, writes, evictions, dropped,
                errors, notAdmitted);
    }

    /**
     * Describes the tier's use so far.
     *
     * @return {@code diskUsedBytes=... diskBlocks=... diskHits=...} and the other counts
     */
    String stats() {
        Use use = use();
        return "diskUsedBytes=" + use.usedBytes() + " diskBlocks=" + use.blocks() + " diskHits=" + use.hits()
                + " diskMisses=" + use.misses() + " diskWrites=" + use.writes() + " diskEvictions=" + use.evictions()
                + " diskDropped=" + use.dropped() + " diskErrors=" + use.errors() + " diskNotAdmitted="
                + use.notAdmitted();
    }

    /** Stops the writer, deletes every block file and releases the directory's lock. */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            index.clear();
            usedBytes = 0;
        }
        writer.interrupt();
        try {
            writer.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pending.clear();
        try {
            deleteBlockFiles(blocksDir);
        } catch (IOException e) {
            LOGGER.warn("{}=cacheDiskCleanupFailed path={} error={}", kind, blocksDir, e.toString());
        }
        releaseLock();
    }

    /** Releases the directory's lock. */
    private void releaseLock() {
        try {
            lock.release();
        } catch (IOException e) {
            LOGGER.debug("{}=cacheDiskUnlockFailed error={}", kind, e.toString());
        }
        try {
            lockChannel.close();
        } catch (IOException e) {
            LOGGER.debug("{}=cacheDiskUnlockFailed error={}", kind, e.toString());
        }
    }

    /**
     * Deletes the block files (and their two-digit directories) this class writes; other files
     * are left alone.
     *
     * @param dir block directory
     * @throws IOException when the directory cannot be listed
     */
    static void deleteBlockFiles(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (DirectoryStream<Path> subdirs = Files.newDirectoryStream(dir)) {
            for (Path sub : subdirs) {
                if (!Files.isDirectory(sub) || !BLOCK_DIR.matcher(String.valueOf(sub.getFileName())).matches()) {
                    continue;
                }
                try (DirectoryStream<Path> files = Files.newDirectoryStream(sub)) {
                    for (Path f : files) {
                        if (BLOCK_FILE.matcher(String.valueOf(f.getFileName())).matches()) {
                            deleteQuietly(f);
                        }
                    }
                }
                try {
                    Files.deleteIfExists(sub);
                } catch (IOException e) {
                    // Not empty: holds files this class did not write.
                }
            }
        }
    }

    /**
     * Restricts a directory to its owner where the file system has POSIX permissions.
     *
     * @param dir directory
     * @throws IOException when the permissions cannot be set
     */
    private static void ownerOnly(Path dir) throws IOException {
        if (Files.getFileStore(dir).supportsFileAttributeView("posix")) {
            Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------"));
        }
    }

    /**
     * Deletes a file, ignoring failures.
     *
     * @param file file
     */
    private static void deleteQuietly(Path file) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOGGER.debug("cache file not deleted: {} ({})", file, e.toString());
        }
    }
}
