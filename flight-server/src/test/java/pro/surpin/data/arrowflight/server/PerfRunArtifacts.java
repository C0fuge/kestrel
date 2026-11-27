package pro.surpin.data.arrowflight.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Collects raw measurements of an in-process performance screening run. */
final class PerfRunArtifacts implements AutoCloseable {
    private final Path directory;
    private final BufferedWriter resources;
    private final ScheduledExecutorService sampler;
    private final jdk.jfr.Recording recording;
    private final AtomicReference<IOException> failure = new AtomicReference<>();
    private final long started = System.nanoTime();

    /**
     * Starts a collector in a fresh output directory.
     *
     * @param directory output directory
     * @throws Exception when the output cannot be opened
     */
    private PerfRunArtifacts(Path directory) throws Exception {
        this.directory = directory;
        Files.createDirectories(directory);
        resources = Files.newBufferedWriter(directory.resolve("resources.csv"), StandardOpenOption.CREATE_NEW);
        resources.write("elapsed_ns,cpu_ns,rss_kib,heap_used,heap_committed,nonheap_used,direct_bytes,"
                + "direct_capacity,mapped_bytes,gc_count,gc_ms,threads\n");
        recording = new jdk.jfr.Recording(jdk.jfr.Configuration.getConfiguration("profile"));
        recording.start();
        String duckdb;
        try (var connection = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                var statement = connection.createStatement();
                var result = statement.executeQuery("SELECT version()")) {
            result.next();
            duckdb = result.getString(1);
        }
        Files.writeString(directory.resolve("runtime.txt"), "java=" + System.getProperty("java.runtime.version")
                + "\nvm=" + System.getProperty("java.vm.name") + "\nduckdb=" + duckdb
                + "\nos=" + System.getProperty("os.name") + " " + System.getProperty("os.version")
                + "\narch=" + System.getProperty("os.arch")
                + "\nprocessors=" + Runtime.getRuntime().availableProcessors()
                + "\nsampler_start_ns=" + started
                + "\narguments=" + ManagementFactory.getRuntimeMXBean().getInputArguments()
                + "\nperf-properties=" + System.getProperties().entrySet().stream()
                        .filter(e -> e.getKey().toString().startsWith("perf.")
                                || e.getKey().toString().startsWith("arrowflight.experimental."))
                        .sorted(java.util.Comparator.comparing(e -> e.getKey().toString())).toList()
                + "\nscope=" + (System.getProperty("perf.server") == null
                        ? "embedded server and clients share one JVM; process memory/CPU includes both"
                        : "external server; process memory/CPU/JFR describe only the clients and data writer") + "\n"
                + "bytes=Arrow vector buffers, excluding transport framing\n", StandardOpenOption.CREATE_NEW);
        sampler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "perf-resource-sampler");
            thread.setDaemon(true);
            return thread;
        });
        sampler.scheduleAtFixedRate(this::sample, 0, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Tells whether raw output was requested.
     *
     * @return true when perf.output is set
     */
    static boolean enabled() {
        return System.getProperty("perf.output") != null;
    }

    /**
     * Starts optional collection for one test invocation.
     *
     * @return collector or null when disabled
     * @throws Exception when output cannot be created
     */
    static PerfRunArtifacts open() throws Exception {
        return enabled() ? new PerfRunArtifacts(Path.of(System.getProperty("perf.output"))) : null;
    }

    /**
     * Writes individual query samples, including warm-up samples.
     *
     * @param label workload step
     * @param samples one buffer per client
     * @throws IOException when writing fails
     */
    static void writeSamples(String label, List<StringBuilder> samples) throws IOException {
        Path path = Path.of(System.getProperty("perf.output"), label + ".csv");
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW)) {
            writer.write("thread,sequence,query_kind,key,start_ns,latency_ns,rows,checksum,arrow_bytes,batches\n");
            for (StringBuilder sample : samples) {
                writer.append(sample);
            }
        }
    }

    /**
     * Records a lifecycle milestone.
     *
     * @param name stage name
     * @throws IOException when writing fails
     */
    void stage(String name) throws IOException {
        Files.writeString(directory.resolve("stages.tsv"), name + "\t" + (System.nanoTime() - started) + "\n",
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** Samples the process and JVM without interrupting the timed query threads. */
    private void sample() {
        try {
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            MemoryUsage nonheap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
            long direct = 0;
            long capacity = 0;
            long mapped = 0;
            for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                if ("direct".equals(pool.getName())) {
                    direct = pool.getMemoryUsed();
                    capacity = pool.getTotalCapacity();
                } else if (pool.getName().startsWith("mapped")) {
                    mapped += pool.getMemoryUsed();
                }
            }
            long count = 0;
            long ms = 0;
            for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                count += Math.max(0, gc.getCollectionCount());
                ms += Math.max(0, gc.getCollectionTime());
            }
            long rss = -1;
            Path status = Path.of("/proc/self/status");
            if (Files.isReadable(status)) {
                for (String line : Files.readAllLines(status)) {
                    if (line.startsWith("VmRSS:")) {
                        rss = Long.parseLong(line.strip().split("\\s+")[1]);
                    }
                }
            }
            long cpu = ((com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
                    .getProcessCpuTime();
            resources.write(String.format(Locale.ROOT, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
                    System.nanoTime() - started, cpu, rss, heap.getUsed(), heap.getCommitted(), nonheap.getUsed(),
                    direct, capacity, mapped, count, ms, ManagementFactory.getThreadMXBean().getThreadCount()));
            resources.flush();
        } catch (IOException e) {
            failure.compareAndSet(null, e);
        } catch (RuntimeException e) {
            failure.compareAndSet(null, new IOException("Performance resource sampling failed", e));
        }
    }

    @Override
    public void close() throws Exception {
        sampler.shutdown();
        if (!sampler.awaitTermination(5, TimeUnit.SECONDS)) {
            sampler.shutdownNow();
            throw new IOException("Performance sampler did not stop");
        }
        resources.close();
        try (recording) {
            recording.stop();
            recording.dump(directory.resolve("profile.jfr"));
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }
}
