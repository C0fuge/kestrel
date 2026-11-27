package pro.surpin.data.arrowflight.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;

import org.apache.arrow.flight.FlightClient;

/** Retains flushed per-query TPC-H client measurements in a fresh output directory. */
final class TpchQuerySamples implements AutoCloseable {
    private final BufferedWriter writer;
    private final long originNanos = System.nanoTime();
    private final long originEpochMillis = System.currentTimeMillis();

    /**
     * Creates the raw files without replacing any previous run.
     *
     * @param directory new output directory
     * @param names query names
     * @param sqls exact executed query texts
     * @param expected expected row counts
     * @param rounds measured rounds
     * @param clients concurrent clients
     * @param mode effective result mode
     * @throws Exception when metadata or output creation fails
     */
    private TpchQuerySamples(Path directory, List<String> names, List<String> sqls, List<Long> expected,
            int rounds, int clients, String mode) throws Exception {
        Files.createDirectory(directory);
        String arrowVersion = FlightClient.class.getPackage().getImplementationVersion();
        StringBuilder metadata = new StringBuilder("java.runtime.version=")
                .append(System.getProperty("java.runtime.version"))
                .append("\njava.vm.name=").append(System.getProperty("java.vm.name"))
                .append("\narrow.java.version=").append(arrowVersion == null ? "unknown" : arrowVersion)
                .append("\nperf.rounds=").append(rounds).append("\nperf.clients=").append(clients)
                .append("\nperf.mode=").append(mode)
                .append("\norigin_epoch_ms=").append(originEpochMillis)
                .append("\norigin_ns=").append(originNanos)
                .append("\nstart_ns=absolute System.nanoTime; epoch timestamp anchored to origins")
                .append("\nfirst_query=first attempt per client, round 0 sequence 0")
                .append("\nchecksum=order-independent normalized row hash, hexadecimal unsigned 64-bit")
                .append("\narrow_bytes=Arrow vector buffers, excluding transport framing")
                .append("\nunknown=rows,arrow_bytes,batches -1 and checksum 0 after incomplete reads\n");
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < names.size(); i++) {
            metadata.append("query.").append(i).append(".name=").append(names.get(i))
                    .append("\nquery.").append(i).append(".expected_rows=").append(expected.get(i))
                    .append("\nquery.").append(i).append(".sha256=")
                    .append(HexFormat.of().formatHex(digest.digest(sqls.get(i).getBytes(StandardCharsets.UTF_8))))
                    .append('\n');
        }
        Files.writeString(directory.resolve("metadata.txt"), metadata, StandardOpenOption.CREATE_NEW);
        writer = Files.newBufferedWriter(directory.resolve("queries.csv"), StandardOpenOption.CREATE_NEW);
        writer.write("query,client,round,warmup,sequence,first_query,start_epoch_ms,start_ns,latency_ns,"
                + "rows,checksum,arrow_bytes,batches,ok,error_type\n");
        writer.flush();
    }

    /**
     * Opens optional raw collection for one benchmark invocation.
     *
     * @param names query names
     * @param sqls exact executed query texts
     * @param expected expected row counts
     * @param rounds measured rounds
     * @param clients concurrent clients
     * @param mode effective result mode
     * @return collector, or null when perf.rawDir is absent
     * @throws Exception when the output directory is not fresh or files cannot be created
     */
    static TpchQuerySamples open(List<String> names, List<String> sqls, List<Long> expected,
            int rounds, int clients, String mode) throws Exception {
        String directory = System.getProperty("perf.rawDir");
        return directory == null ? null
                : new TpchQuerySamples(Path.of(directory), names, sqls, expected, rounds, clients, mode);
    }

    /**
     * Flushes one attempted query, including warm-up, mismatches and failed reads.
     *
     * @param query query name
     * @param client client index
     * @param round round number, zero for warm-up
     * @param sequence position within the client's round
     * @param start absolute monotonic query start
     * @param elapsed measured query duration
     * @param answer completely read answer, or null when the read did not complete
     * @param error empty on success, otherwise a safe error type
     * @throws IOException when a complete sample cannot be retained
     */
    synchronized void write(String query, int client, int round, int sequence, long start, long elapsed,
            Answer answer, String error) throws IOException {
        long epochMillis = originEpochMillis + (start - originNanos) / 1_000_000;
        writer.write(String.format(Locale.ROOT, "\"%s\",%d,%d,%d,%d,%d,%d,%d,%d,%d,%016x,%d,%d,%d,%s%n",
                query.replace("\"", "\"\""), client, round, round == 0 ? 1 : 0, sequence,
                round == 0 && sequence == 0 ? 1 : 0, epochMillis, start, elapsed,
                answer == null ? -1 : answer.rows(), answer == null ? 0 : answer.checksum(),
                answer == null ? -1 : answer.bytes(), answer == null ? -1 : answer.batches(),
                error.isEmpty() ? 1 : 0, error));
        writer.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }

    /**
     * One fully consumed answer and optional Arrow transfer measurements.
     *
     * @param rows returned rows
     * @param checksum normalized order-independent value checksum
     * @param bytes Arrow buffer bytes, or -1 when collection is disabled
     * @param batches Arrow batches, or -1 when collection is disabled
     */
    record Answer(long rows, long checksum, long bytes, long batches) {
    }
}
