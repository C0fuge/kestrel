package pro.surpin.data.arrowflight.server.adapters;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default DuckDB threads and memory limit per connection from the host's cores and memory.
 * Every worker connection is a DuckDB instance of its own, so the node's DuckDB memory budget
 * (memory minus heap, direct memory and 15 % headroom) is split between the connections
 * expected to run at once. Threads are a quarter of the cores, at least 4; above 4 they are
 * capped at one per GiB of that share. Explicit settings always win.
 *
 * @param threads             DuckDB threads per connection
 * @param memoryLimit         DuckDB {@code memory_limit} per connection, null = DuckDB's default
 * @param coreThreads         threads the cores alone would give
 * @param connections         connections expected to run at once
 * @param budgetBytes         node's DuckDB memory budget, may be negative or 0 when unknown
 * @param host                host limits used
 */
public record DuckDbSizing(int threads, String memoryLimit, int coreThreads, int connections,
        long budgetBytes, HostLimits host) {

    /** Value of {@code duckDbMemoryLimit} that leaves DuckDB's own default (80 % of RAM per instance). */
    public static final String MEMORY_LIMIT_NONE = "none";

    /**
     * Fewest default threads per connection (the fixed default before item 19). Fewer made
     * TPC-H SF10 1.35-1.7x slower on 4-16 core containers, whatever the memory limit.
     */
    static final int MIN_THREADS = 4;

    /**
     * Fewest threads assumed per query when the memory budget is split between connections: the
     * split keeps the shares item 19b measured (cores / 4, at least 2) although the threads have
     * a floor of 4.
     */
    static final int SHARE_MIN_THREADS = 2;

    /** Fewest default threads on object stores read over HTTP, whose reads wait on round trips. */
    static final int REMOTE_MIN_THREADS = 4;

    /** Most default threads per connection. */
    static final int MAX_THREADS = 16;

    /** Smallest default memory limit per connection. */
    static final long MIN_CONNECTION_BYTES = 1L << 30;

    /** Memory per DuckDB thread for joins and aggregates (DuckDB's guidance). */
    static final long BYTES_PER_THREAD = 1L << 30;

    /** Share of the host memory kept free for the OS, metaspace, thread stacks and native libraries. */
    static final int HEADROOM_PERCENT = 15;

    /** Most of the host memory one connection gets by default (DuckDB's own default share). */
    static final int MAX_CONNECTION_PERCENT = 80;

    private static final Pattern SIZE = Pattern.compile(
            "\\s*(\\d+(?:\\.\\d+)?)\\s*(B|KB|MB|GB|TB|KIB|MIB|GIB|TIB)?\\s*");

    /**
     * Computes the sizing.
     *
     * @param host                 host limits
     * @param objectStore          whether the data directory is an object store read over HTTP
     * @param explicitThreads      configured {@code duckDbThreads}, 0 = unset
     * @param explicitMemoryLimit  configured {@code duckDbMemoryLimit}, null or blank = unset
     * @param explicitConnections  configured {@code duckDbMemoryConnections}, 0 = unset
     * @return sizing
     */
    public static DuckDbSizing compute(HostLimits host, boolean objectStore, int explicitThreads,
            String explicitMemoryLimit, int explicitConnections) {
        int cores = Math.max(1, host.cores());
        int coreThreads = Math.min(MAX_THREADS, Math.max(MIN_THREADS, cores / 4));
        int shareThreads = Math.min(MAX_THREADS, Math.max(SHARE_MIN_THREADS, cores / 4));
        if (objectStore) {
            coreThreads = Math.max(REMOTE_MIN_THREADS, coreThreads);
            shareThreads = Math.max(REMOTE_MIN_THREADS, shareThreads);
        }
        int connections = explicitConnections > 0 ? explicitConnections
                : Math.max(2, cores / shareThreads);
        long memory = host.memoryBytes();
        long budget = memory <= 0 ? 0
                : memory - host.maxHeapBytes() - host.maxDirectBytes() - memory / 100 * HEADROOM_PERCENT;

        String memoryLimit;
        Long limitBytes;
        if (explicitMemoryLimit != null && !explicitMemoryLimit.isBlank()) {
            boolean none = MEMORY_LIMIT_NONE.equalsIgnoreCase(explicitMemoryLimit.trim());
            memoryLimit = none ? null : explicitMemoryLimit.trim();
            limitBytes = none ? null : parseBytes(memoryLimit);
        } else if (memory <= 0) {
            memoryLimit = null;
            limitBytes = null;
        } else {
            long perConnection = Math.max(MIN_CONNECTION_BYTES, budget / connections);
            perConnection = Math.min(perConnection, memory / 100 * MAX_CONNECTION_PERCENT);
            long mib = Math.max(1, perConnection >> 20);
            memoryLimit = mib + "MiB";
            limitBytes = mib << 20;
        }

        int threads;
        if (explicitThreads > 0) {
            threads = explicitThreads;
        } else if (limitBytes == null) {
            threads = coreThreads;
        } else {
            long byMemory = limitBytes / BYTES_PER_THREAD;
            threads = (int) Math.max(MIN_THREADS, Math.min(coreThreads, byMemory));
        }
        return new DuckDbSizing(threads, memoryLimit, coreThreads, connections, budget, host);
    }

    /**
     * Node-wide DuckDB threads and memory limit of {@code duckDbSharedDatabase}, where every
     * worker connection is a connection to one DuckDB of the node.
     *
     * @param threads             DuckDB threads of the node
     * @param memoryLimit         DuckDB {@code memory_limit} of the node, null = DuckDB's default
     * @param bufferedConnections connections opened before the lockdown with
     *                            {@code duckDbStreamingBufferSize} set, used only with that setting
     */
    public record Shared(int threads, String memoryLimit, int bufferedConnections) {

        /** Default of {@code duckDbSharedBufferedConnections}. */
        public static final int DEFAULT_BUFFERED_CONNECTIONS = 64;

        /**
         * Creates the node-wide sizing with the default number of buffered connections.
         *
         * @param threads     DuckDB threads of the node
         * @param memoryLimit DuckDB {@code memory_limit} of the node, null = DuckDB's default
         */
        public Shared(int threads, String memoryLimit) {
            this(threads, memoryLimit, DEFAULT_BUFFERED_CONNECTIONS);
        }

        /**
         * Returns this sizing with another number of buffered connections.
         *
         * @param connections connections opened before the lockdown, 0 = none
         * @return sizing with the given number
         */
        public Shared withBufferedConnections(int connections) {
            return new Shared(threads, memoryLimit, connections);
        }

        /**
         * Describes the chosen values for the startup log.
         *
         * @return one line of key=value pairs
         */
        public String describe() {
            return "threads=" + threads + " memoryLimit=" + (memoryLimit == null ? "duckdbDefault" : memoryLimit);
        }
    }

    /**
     * Computes the node-wide sizing of one shared DuckDB: the threads are the cores and the
     * memory limit is the whole DuckDB budget (at least 1 GiB, at most 80 % of the memory).
     * Explicit settings win and then apply to the node, not to a connection.
     *
     * @param host                host limits
     * @param explicitThreads     configured {@code duckDbThreads}, 0 = unset
     * @param explicitMemoryLimit configured {@code duckDbMemoryLimit}, null or blank = unset
     * @return node-wide sizing
     */
    public static Shared shared(HostLimits host, int explicitThreads, String explicitMemoryLimit) {
        int threads = explicitThreads > 0 ? explicitThreads : Math.max(1, host.cores());
        String memoryLimit;
        if (explicitMemoryLimit != null && !explicitMemoryLimit.isBlank()) {
            memoryLimit = MEMORY_LIMIT_NONE.equalsIgnoreCase(explicitMemoryLimit.trim()) ? null
                    : explicitMemoryLimit.trim();
        } else if (host.memoryBytes() <= 0) {
            memoryLimit = null;
        } else {
            long memory = host.memoryBytes();
            long budget = memory - host.maxHeapBytes() - host.maxDirectBytes()
                    - memory / 100 * HEADROOM_PERCENT;
            long bytes = Math.min(Math.max(MIN_CONNECTION_BYTES, budget), memory / 100 * MAX_CONNECTION_PERCENT);
            memoryLimit = Math.max(1, bytes >> 20) + "MiB";
        }
        return new Shared(threads, memoryLimit);
    }

    /**
     * Parses a DuckDB size such as {@code 4GB}, {@code 512MiB} or {@code 1073741824}.
     *
     * @param text size text
     * @return bytes, or null when the text is no plain size (e.g. a percentage)
     */
    static Long parseBytes(String text) {
        Matcher m = SIZE.matcher(text.toUpperCase(Locale.ROOT));
        if (!m.matches()) {
            return null;
        }
        double value = Double.parseDouble(m.group(1));
        String unit = m.group(2) == null ? "B" : m.group(2);
        long factor = switch (unit) {
            case "KB" -> 1_000L;
            case "MB" -> 1_000_000L;
            case "GB" -> 1_000_000_000L;
            case "TB" -> 1_000_000_000_000L;
            case "KIB" -> 1L << 10;
            case "MIB" -> 1L << 20;
            case "GIB" -> 1L << 30;
            case "TIB" -> 1L << 40;
            default -> 1L;
        };
        return (long) (value * factor);
    }

    /**
     * Describes how far DuckDB's external file cache ({@code duckDbExternalFileCache=true}) may
     * grow. DuckDB keeps it in each connection's buffer pool and evicts it within
     * {@code memory_limit}, but an idle connection keeps what it cached, so the node may hold up
     * to the limit times its live connections (every Flight worker thread that ran a query, until
     * its connection is closed); without a limit each connection may cache up to DuckDB's default
     * of 80 % of the host memory.
     *
     * @return one line of key=value pairs for the startup log
     */
    public String describeExternalFileCache() {
        Long limitBytes = memoryLimit == null ? null : parseBytes(memoryLimit);
        String expected = limitBytes == null ? "unknown" : Long.toString((limitBytes >> 20) * connections);
        return "perConnection=" + (memoryLimit == null ? "duckdbDefault" : memoryLimit)
                + " expectedConnections=" + connections + " expectedMiB=" + expected
                + " memoryMiB=" + (host.memoryBytes() >> 20)
                + " bound=memoryLimitTimesLiveConnections";
    }

    /**
     * Tells whether DuckDB's external file cache has no per-connection bound below DuckDB's own
     * default (80 % of the host memory per connection), the case that got servers OOM-killed.
     *
     * @return true when no {@code memory_limit} is set
     */
    public boolean externalFileCacheUnbounded() {
        return memoryLimit == null;
    }

    /**
     * Describes the inputs and the chosen values for the startup log.
     *
     * @return one line of key=value pairs
     */
    public String describe() {
        return "threads=" + threads + " memoryLimit=" + (memoryLimit == null ? "duckdbDefault" : memoryLimit)
                + " coreThreads=" + coreThreads + " connections=" + connections
                + " budgetMiB=" + (budgetBytes >> 20) + " cores=" + host.cores()
                + " memoryMiB=" + (host.memoryBytes() >> 20) + " heapMiB=" + (host.maxHeapBytes() >> 20)
                + " directMiB=" + (host.maxDirectBytes() >> 20);
    }
}
