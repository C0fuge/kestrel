package pro.surpin.data.arrowflight.server.adapters;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Default DuckDB threads and memory limit per connection from the host's cores and memory.
 * Every worker connection is a DuckDB instance of its own, so the node's DuckDB memory budget
 * (memory minus heap, direct memory and 15 % headroom) is split between the connections
 * expected to run at once. Threads are a quarter of the cores, at least 4; above 4 they are
 * capped at one per GiB of that share. A connection gets at least 1 GiB, but never more than its
 * share of the budget left by the JVM's real reservation (item 44: the heap and direct memory the
 * JVM can really use, as under {@code speed}; item 46: the connections' limits together stay
 * within that budget), and never less than 512 MiB. Explicit settings always win.
 *
 * @param threads             DuckDB threads per connection
 * @param memoryLimit         DuckDB {@code memory_limit} per connection, null = DuckDB's default
 * @param coreThreads         threads the cores alone would give
 * @param connections         connections expected to run at once
 * @param budgetBytes         DuckDB budget from the full -Xmx and direct memory, may be negative or 0 when
 *                            unknown; the defaults are sized from {@link #reservedBudgetBytes()}
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

    /**
     * Smallest default memory limit when the DuckDB budget is smaller than
     * {@link #MIN_CONNECTION_BYTES}: a limit above the budget got speed servers OOM-killed in a
     * 3 GiB container (item 42), so the budget wins over the 1 GiB floor down to this size, under
     * {@code speed}, per connection and for the shared DuckDB (Q-42).
     */
    static final long MIN_BUDGET_BYTES = 128L << 20;

    /**
     * Smallest default memory limit of a connection and of the shared DuckDB without
     * {@code speed} (item 44): Q-42 capped them at the budget left by the full -Xmx and direct
     * memory, down to 128 MiB, and balanced then failed TPC-H SF10 q09/q10/q13 with DuckDB out of
     * memory in a 6 GB container with -Xmx3g / direct 2g, which had passed 22/22 with 1 GiB.
     */
    static final long MIN_USABLE_BYTES = 512L << 20;

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
            // Never above this connection's share of what the JVM's real reservation leaves (items
            // 44, 46), at least 512 MiB: the full -Xmx3g + direct 2g left ~100 MiB in 6 GB and the
            // Q-42 cap at that failed TPC-H; 4 x 1 GiB in a 2.6 GiB reserved budget added up above it.
            perConnection = withinReservedShare(perConnection, reservedBudget(host), connections);
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

    /** Default of {@code duckDbSharedMaxThreads}: most threads of a shared DuckDB unless set. */
    public static final int DEFAULT_SHARED_MAX_THREADS = 8;

    /**
     * Node-wide DuckDB threads and memory limit of {@code duckDbSharedDatabase}, where every
     * worker connection is a connection to one DuckDB of the node, and the size of the private
     * DuckDB a query runs on again when the shared one ran out of memory before sending anything.
     *
     * @param threads             DuckDB threads of the node
     * @param memoryLimit         DuckDB {@code memory_limit} of the node, null = DuckDB's default
     * @param bufferedConnections connections opened before the lockdown with
     *                            {@code duckDbStreamingBufferSize} set, used only with that setting
     * @param fallbackThreads     DuckDB threads of a private DuckDB for a query run again
     * @param fallbackMemoryLimit DuckDB {@code memory_limit} of that private DuckDB, null = DuckDB's default
     */
    public record Shared(int threads, String memoryLimit, int bufferedConnections, int fallbackThreads,
            String fallbackMemoryLimit) {

        /** Default of {@code duckDbSharedBufferedConnections}. */
        public static final int DEFAULT_BUFFERED_CONNECTIONS = 64;

        /**
         * Creates the node-wide sizing with the default number of buffered connections and a
         * private DuckDB of the same size.
         *
         * @param threads     DuckDB threads of the node
         * @param memoryLimit DuckDB {@code memory_limit} of the node, null = DuckDB's default
         */
        public Shared(int threads, String memoryLimit) {
            this(threads, memoryLimit, DEFAULT_BUFFERED_CONNECTIONS);
        }

        /**
         * Creates the node-wide sizing with a private DuckDB of the same size.
         *
         * @param threads             DuckDB threads of the node
         * @param memoryLimit         DuckDB {@code memory_limit} of the node, null = DuckDB's default
         * @param bufferedConnections connections opened before the lockdown, 0 = none
         */
        public Shared(int threads, String memoryLimit, int bufferedConnections) {
            this(threads, memoryLimit, bufferedConnections, threads, memoryLimit);
        }

        /**
         * Returns this sizing with another number of buffered connections.
         *
         * @param connections connections opened before the lockdown, 0 = none
         * @return sizing with the given number
         */
        public Shared withBufferedConnections(int connections) {
            return new Shared(threads, memoryLimit, connections, fallbackThreads, fallbackMemoryLimit);
        }

        /**
         * Returns this sizing with the private DuckDB sized as one connection of the
         * DuckDB-per-connection mode.
         *
         * @param perConnection sizing of the DuckDB-per-connection mode
         * @return sizing with that private DuckDB
         */
        public Shared withFallback(DuckDbSizing perConnection) {
            return new Shared(threads, memoryLimit, bufferedConnections, perConnection.threads(),
                    perConnection.memoryLimit());
        }

        /**
         * Describes how far DuckDB's external file cache may grow with the shared DuckDB: within
         * its one memory limit, evicted like any other buffer; the private DuckDB of queries that
         * ran out of memory keeps none.
         *
         * @return one line of key=value pairs for the startup log
         */
        public String describeExternalFileCache() {
            return "nodeWide=" + (memoryLimit == null ? "duckdbDefault" : memoryLimit)
                    + " privateDatabase=off bound=sharedMemoryLimit";
        }

        /**
         * Describes the chosen values for the startup log.
         *
         * @return one line of key=value pairs
         */
        public String describe() {
            return "threads=" + threads + " memoryLimit=" + (memoryLimit == null ? "duckdbDefault" : memoryLimit)
                    + " fallbackThreads=" + fallbackThreads + " fallbackMemoryLimit="
                    + (fallbackMemoryLimit == null ? "duckdbDefault" : fallbackMemoryLimit);
        }
    }

    /**
     * Computes the node-wide sizing of one shared DuckDB with the default thread cap
     * ({@link #DEFAULT_SHARED_MAX_THREADS}).
     *
     * @param host                host limits
     * @param explicitThreads     configured {@code duckDbThreads}, 0 = unset
     * @param explicitMemoryLimit configured {@code duckDbMemoryLimit}, null or blank = unset
     * @return node-wide sizing
     */
    public static Shared shared(HostLimits host, int explicitThreads, String explicitMemoryLimit) {
        return shared(host, explicitThreads, explicitMemoryLimit, DEFAULT_SHARED_MAX_THREADS);
    }

    /**
     * Computes the node-wide sizing of one shared DuckDB: the threads are the cores up to a cap
     * (more threads on a large node lengthened small-query tails on the stand) and the memory
     * limit is the whole DuckDB budget (at most 80 % of the memory, at least 1 GiB unless the
     * budget left by the JVM's real reservation is smaller, then at least
     * {@link #MIN_USABLE_BYTES}, item 44). Explicit
     * settings win and then apply to the node, not to a connection.
     *
     * @param host                host limits
     * @param explicitThreads     configured {@code duckDbThreads}, 0 = unset
     * @param explicitMemoryLimit configured {@code duckDbMemoryLimit}, null or blank = unset
     * @param maxThreads          configured {@code duckDbSharedMaxThreads}, 0 = no cap
     * @return node-wide sizing
     */
    public static Shared shared(HostLimits host, int explicitThreads, String explicitMemoryLimit, int maxThreads) {
        int cores = Math.max(1, host.cores());
        int threads = explicitThreads > 0 ? explicitThreads : maxThreads > 0 ? Math.min(cores, maxThreads) : cores;
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
            bytes = withinReservedBudget(bytes, host);
            memoryLimit = Math.max(1, bytes >> 20) + "MiB";
        }
        return new Shared(threads, memoryLimit);
    }

    /**
     * Caps a default DuckDB memory limit at the DuckDB budget (memory minus heap, direct memory
     * and headroom), never below {@link #MIN_BUDGET_BYTES}: the 1 GiB floor and DuckDB's 80 %
     * share must not let DuckDB, the heap and direct memory together exceed the container
     * (item 42: 2457 MiB + 768 MiB heap + 512 MiB direct in 3 GiB, OOM kill).
     *
     * @param bytes  limit before the cap
     * @param budget DuckDB budget in bytes, may be negative
     * @return the limit, at most the budget
     */
    static long withinBudget(long bytes, long budget) {
        return Math.min(bytes, Math.max(MIN_BUDGET_BYTES, budget));
    }

    /**
     * Returns the DuckDB budget left by the JVM's real reservation of item 40(a) (heap =
     * min(-Xmx, max(1 GiB, heap in use x 2)), direct = min(limit, 1 GiB + 25 %)) instead of
     * the full -Xmx and direct memory, minus the headroom.
     *
     * @param host host limits with known memory
     * @return budget in bytes, may be negative
     */
    static long reservedBudget(HostLimits host) {
        HostLimits reserved = ResourceProfile.speedReservation(host);
        long memory = host.memoryBytes();
        return memory - reserved.maxHeapBytes() - reserved.maxDirectBytes() - memory / 100 * HEADROOM_PERCENT;
    }

    /**
     * Caps a default memory limit of a connection or of the shared DuckDB without {@code speed}
     * at the budget left by the JVM's real reservation, never below {@link #MIN_USABLE_BYTES}
     * (item 44).
     *
     * @param bytes limit before the cap
     * @param host  host limits with known memory
     * @return the limit, at most that budget unless it is below the usable floor
     */
    static long withinReservedBudget(long bytes, HostLimits host) {
        return Math.min(bytes, Math.max(MIN_USABLE_BYTES, reservedBudget(host)));
    }

    /**
     * Caps a default memory limit of one of several DuckDB connections at its share of the budget
     * left by the JVM's real reservation, never below {@link #MIN_USABLE_BYTES} (item 46: the
     * limits of the connections expected at once must not add up above that budget).
     *
     * @param bytes       limit before the cap
     * @param budget      budget left by the JVM's real reservation, may be negative
     * @param connections connections expected at once
     * @return the limit, at most the share unless the share is below the usable floor
     */
    static long withinReservedShare(long bytes, long budget, int connections) {
        return Math.min(bytes, Math.max(MIN_USABLE_BYTES, budget / Math.max(1, connections)));
    }

    /**
     * Returns the DuckDB budget the default limits are sized from: memory minus the JVM's real
     * reservation of item 40(a) and the headroom (item 46: the same number for the cap, the
     * start log and the WARN).
     *
     * @return budget in bytes, 0 when the memory is unknown, may be negative
     */
    public long reservedBudgetBytes() {
        return host.memoryBytes() <= 0 ? 0 : reservedBudget(host);
    }

    /**
     * Describes default DuckDB memory limits that together exceed the DuckDB budget left by the
     * JVM's real reservation (items 44, 46), for a WARN at start: only the 512 MiB floor lets
     * that happen, and the server then relies on the JVM using even less. An operator's
     * {@code duckDbMemoryLimit} is never warned about.
     *
     * @param appliedLimit memory limit of one DuckDB, null = DuckDB's default
     * @param instances    DuckDB instances expected at once (connections, 1 for a shared DuckDB)
     * @param budgetBytes  DuckDB budget left by the JVM's real reservation ({@link #reservedBudgetBytes()})
     * @param operatorSet  whether the operator set {@code duckDbMemoryLimit}
     * @return key=value pairs with a hint, or null when there is nothing to warn about
     */
    public static String aboveJvmLimitsWarning(String appliedLimit, int instances, long budgetBytes,
            boolean operatorSet) {
        Long bytes = appliedLimit == null ? null : parseBytes(appliedLimit);
        if (operatorSet || bytes == null || bytes * Math.max(1, instances) <= budgetBytes) {
            return null;
        }
        return "memoryLimit=" + appliedLimit + " instances=" + Math.max(1, instances)
                + " budgetMiB=" + (budgetBytes >> 20)
                + " hint=the JVM's reservation (heap in use x 2, at least 1 GiB; direct 1 GiB + 25 %) leaves"
                + " less than these limits at the 512 MiB floor; the container may be OOM-killed: give it more"
                + " memory, lower duckDbMemoryConnections, -Xmx / -XX:MaxDirectMemorySize or set duckDbMemoryLimit";
    }

    /**
     * Describes a default DuckDB memory limit that the budget cut below the old 1 GiB floor
     * (Q-42), for a WARN at start; an operator's {@code duckDbMemoryLimit} is never warned about.
     *
     * @param appliedLimit memory limit DuckDB gets, null = DuckDB's default
     * @param operatorSet  whether the operator set {@code duckDbMemoryLimit}
     * @return key=value pairs with a hint naming the setting, or null when there is nothing to warn about
     */
    public static String belowFloorWarning(String appliedLimit, boolean operatorSet) {
        Long bytes = appliedLimit == null ? null : parseBytes(appliedLimit);
        if (operatorSet || bytes == null || bytes >= MIN_CONNECTION_BYTES) {
            return null;
        }
        return "memoryLimit=" + appliedLimit + " floorMiB=" + (MIN_CONNECTION_BYTES >> 20)
                + " hint=the DuckDB budget (memory - heap - direct - headroom) is below the floor;"
                + " set duckDbMemoryLimit, give the container more memory or lower -Xmx / -XX:MaxDirectMemorySize";
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
                + " budgetMiB=" + (reservedBudgetBytes() >> 20) + " limitsBudgetMiB=" + (budgetBytes >> 20)
                + " cores=" + host.cores()
                + " memoryMiB=" + (host.memoryBytes() >> 20) + " heapMiB=" + (host.maxHeapBytes() >> 20)
                + " directMiB=" + (host.maxDirectBytes() >> 20);
    }
}
