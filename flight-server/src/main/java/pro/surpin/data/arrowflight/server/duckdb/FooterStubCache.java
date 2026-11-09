package pro.surpin.data.arrowflight.server.duckdb;

import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Keeps one footer stub per stored file across queries, so that describing a query against a
 * table's sample file reads the footer from storage once per file version instead of once per
 * query (item 39 (c): every point query with a new literal opened the sample file on HDFS).
 * A stub is reused while the file's location, length and modification time stay the same; a
 * stub that is replaced or evicted is removed once no describe uses it any more.
 */
public final class FooterStubCache implements AutoCloseable {

    /** Default number of stubs kept. */
    public static final int DEFAULT_CAPACITY = 1024;

    private final Path root;
    private final int capacity;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private boolean closed;

    /**
     * Opens the factory of a storage-backed input file.
     */
    @FunctionalInterface
    public interface Source {

        /**
         * Opens the stored file.
         *
         * @return input file read through the storage client
         * @throws IOException on storage access failure
         */
        InputFile open() throws IOException;
    }

    /**
     * Creates an empty cache.
     *
     * @param root directory DuckDB may read, below which stubs are written
     * @param capacity largest number of stubs kept
     */
    public FooterStubCache(Path root, int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1: " + capacity);
        }
        this.root = root;
        this.capacity = capacity;
    }

    /**
     * Tells whether describing against a stub saves a storage round trip for a location.
     *
     * @param location DuckDB location of a stored file
     * @return true for remote locations (a URI scheme other than {@code file})
     */
    public static boolean remote(String location) {
        int colon = location.indexOf("://");
        return colon > 1 && !location.regionMatches(true, 0, "file", 0, colon);
    }

    /**
     * Returns the stub of a stored file, writing it on first use or when the file changed.
     *
     * @param location the stored file's DuckDB location
     * @param size the stored file's length
     * @param modificationTime the stored file's modification time
     * @param source opens the stored file when its stub must be written
     * @return lease of the stub, to close when DuckDB has read it
     * @throws IOException when the footer cannot be read or the stub cannot be written
     */
    public Lease lease(String location, long size, long modificationTime, Source source)
            throws IOException {
        synchronized (this) {
            Entry entry = entries.get(location);
            if (entry != null && entry.matches(size, modificationTime)) {
                entry.leases++;
                return new Lease(entry);
            }
        }
        FooterStub written = FooterStub.write(source.open(), root, location);
        Entry fresh = new Entry(written, size, modificationTime);
        synchronized (this) {
            Entry entry = entries.get(location);
            if (closed || entry != null && entry.matches(size, modificationTime)) {
                written.close();
                if (closed) {
                    throw new IOException("Footer stub cache is closed");
                }
                entry.leases++;
                return new Lease(entry);
            }
            if (entry != null) {
                retire(entry);
            }
            entries.put(location, fresh);
            fresh.leases++;
            Iterator<Map.Entry<String, Entry>> eldest = entries.entrySet().iterator();
            while (entries.size() > capacity && eldest.hasNext()) {
                Entry old = eldest.next().getValue();
                eldest.remove();
                retire(old);
            }
            return new Lease(fresh);
        }
    }

    /**
     * Returns the number of stubs kept.
     *
     * @return cached stubs
     */
    public synchronized int size() {
        return entries.size();
    }

    /** Removes every stub that no describe uses; the others go when their lease closes. */
    @Override
    public synchronized void close() {
        closed = true;
        for (Entry entry : entries.values()) {
            retire(entry);
        }
        entries.clear();
    }

    private void retire(Entry entry) {
        entry.retired = true;
        if (entry.leases == 0) {
            entry.stub.close();
        }
    }

    private synchronized void release(Entry entry) {
        entry.leases--;
        if (entry.retired && entry.leases == 0) {
            entry.stub.close();
        }
    }

    /** One stub with its file version and the number of describes using it. */
    private static final class Entry {
        private final FooterStub stub;
        private final long size;
        private final long modificationTime;
        private int leases;
        private boolean retired;

        Entry(FooterStub stub, long size, long modificationTime) {
            this.stub = stub;
            this.size = size;
            this.modificationTime = modificationTime;
        }

        boolean matches(long otherSize, long otherModificationTime) {
            return size == otherSize && modificationTime == otherModificationTime;
        }
    }

    /** Use of a cached stub; the stub stays on disk until every lease of it is closed. */
    public final class Lease implements AutoCloseable {
        private final Entry entry;
        private boolean released;

        private Lease(Entry entry) {
            this.entry = entry;
        }

        /**
         * Returns the stub file.
         *
         * @return local path DuckDB reads
         */
        public Path file() {
            return entry.stub.file();
        }

        /** Releases the stub. */
        @Override
        public void close() {
            if (!released) {
                released = true;
                release(entry);
            }
        }
    }
}
