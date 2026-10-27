package pro.surpin.data.arrowflight.server.metrics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import pro.surpin.data.arrowflight.server.LogUtil;
import pro.surpin.data.arrowflight.server.model.ExecutionPath;
import pro.surpin.data.arrowflight.server.model.ExecutionPathTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Emits structured runtime execution-path evidence for benchmark collection.
 * The log line and the evidence file are written by one background thread, so a DoGet does not
 * wait for the console; when the backlog is full the caller writes its event itself.
 */
public final class ExecutionPathRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPathRecorder.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Object FILE_LOCK = new Object();
    private static final String OUTPUT_ENV = "BENCHMARK_EXECUTION_PATH_LOG";
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern LINE_BREAKS = Pattern.compile("[\\r\\n]+");
    /** One SHA-256 digest per thread: looking up the provider costs more than the hash. */
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    });

    /** Events serialized but not yet written; one writer thread keeps their order. */
    private static final BlockingQueue<Runnable> PENDING = new ArrayBlockingQueue<>(8192);

    static {
        Thread writer = new Thread(ExecutionPathRecorder::writeLoop, "execution-path-log");
        writer.setDaemon(true);
        writer.start();
        Runtime.getRuntime().addShutdownHook(
                new Thread(ExecutionPathRecorder::drain, "execution-path-log-drain"));
    }

    private ExecutionPathRecorder() {
    }

    /**
     * Emits one execution-path event to logs and the optional JSONL evidence file.
     *
     * @param qid endpoint query identifier
     * @param query logical SQL query
     * @param tracker runtime path tracker
     * @param success whether execution completed successfully
     * @param failureReason failure description or null
     */
    public static void recordEvent(String qid, String query, ExecutionPathTracker tracker,
            boolean success, String failureReason) {
        String output = System.getenv(OUTPUT_ENV);
        recordEvent(qid, query, tracker, success, failureReason,
                output == null || output.isBlank() ? null : Path.of(output));
    }

    /**
     * Emits one execution-path event to logs and the given JSONL evidence file.
     *
     * @param qid endpoint query identifier
     * @param query logical SQL query
     * @param tracker runtime path tracker
     * @param success whether execution completed successfully
     * @param failureReason failure description or null
     * @param output JSONL evidence file, or null for none
     */
    static void recordEvent(String qid, String query, ExecutionPathTracker tracker,
            boolean success, String failureReason, Path output) {
        if (!LOGGER.isInfoEnabled() && output == null) {
            return;
        }
        String event = toJson(qid, query, tracker, success, failureReason);
        ExecutionPath path = tracker.path();
        String node = LogUtil.node();
        Runnable write = () -> write(qid, node, path, success, event, output);
        if (!PENDING.offer(write)) {
            write.run();
        }
    }

    /**
     * Writes one serialized event to the log and the optional evidence file.
     *
     * @param qid endpoint query identifier
     * @param node node that ran the query
     * @param path execution path taken
     * @param success whether execution completed successfully
     * @param event serialized event
     * @param output JSONL evidence file, or null for none
     */
    private static void write(String qid, String node, ExecutionPath path, boolean success,
            String event, Path output) {
        LOGGER.info("qid={} node={} executionPath={} pushdownEvidence={} success={} event={}",
                qid, node, path.label(), path.isPushdownEvidence(), success, event);
        if (output == null) {
            return;
        }
        try {
            append(output, event);
        } catch (IOException e) {
            LOGGER.warn("qid={} node={} Could not append execution-path evidence to {}",
                    qid, node, output, e);
        }
    }

    /** Writes queued events until the process ends. */
    private static void writeLoop() {
        while (true) {
            try {
                PENDING.take().run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                LOGGER.warn("Could not write an execution-path event", e);
            }
        }
    }

    /** Writes the events still queued, at shutdown. */
    private static void drain() {
        Runnable write;
        while ((write = PENDING.poll()) != null) {
            try {
                write.run();
            } catch (RuntimeException e) {
                LOGGER.warn("Could not write an execution-path event", e);
            }
        }
    }

    /**
     * Waits until every event recorded before this call is written.
     *
     * @param timeoutMillis maximum wait
     * @return true when written in time
     * @throws InterruptedException when interrupted while waiting
     */
    static boolean flush(long timeoutMillis) throws InterruptedException {
        CountDownLatch written = new CountDownLatch(1);
        if (!PENDING.offer(written::countDown, timeoutMillis, TimeUnit.MILLISECONDS)) {
            return false;
        }
        return written.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Serializes one execution-path event as a JSON object.
     *
     * @param qid endpoint query identifier
     * @param query logical SQL query
     * @param tracker runtime path tracker
     * @param success whether execution completed successfully
     * @param failureReason failure description or null
     * @return serialized event
     */
    public static String toJson(String qid, String query, ExecutionPathTracker tracker,
            boolean success, String failureReason) {
        ExecutionPath path = tracker.path();
        ObjectNode event = JSON.createObjectNode();
        event.put("schema_version", "1.0.0");
        event.put("timestamp", Instant.now().toString());
        event.put("qid", qid);
        event.put("node", LogUtil.node());
        event.put("query_digest", queryDigest(query));
        event.put("execution_path", path.label());
        event.put("pushdown_evidence", path.isPushdownEvidence());
        event.put("success", success);
        putNullable(event, "fallback_target",
                tracker.fallbackTarget() == null ? null : tracker.fallbackTarget().label());
        putNullable(event, "reason", tracker.reason());
        putNullable(event, "failure_reason", sanitize(failureReason));
        try {
            return JSON.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize execution-path event", e);
        }
    }

    /**
     * Computes the stable digest of a whitespace-normalized SQL query.
     *
     * @param query SQL query
     * @return lowercase SHA-256 digest
     */
    public static String queryDigest(String query) {
        String normalized = query == null ? "" : WHITESPACE.matcher(query.strip())
                .replaceAll(" ")
                .toLowerCase(Locale.ROOT);
        MessageDigest digest = SHA256.get();
        digest.reset();
        return HexFormat.of().formatHex(digest.digest(normalized.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Writes a nullable string field.
     *
     * @param event destination JSON object
     * @param field field name
     * @param value field value
     */
    private static void putNullable(ObjectNode event, String field, String value) {
        if (value == null) {
            event.putNull(field);
        } else {
            event.put(field, value);
        }
    }

    /**
     * Removes line breaks and bounds a failure description.
     *
     * @param value untrusted failure description
     * @return sanitized description or null
     */
    private static String sanitize(String value) {
        if (value == null) {
            return null;
        }
        String sanitized = LINE_BREAKS.matcher(value).replaceAll(" ").strip();
        return sanitized.length() <= 1024 ? sanitized : sanitized.substring(0, 1024);
    }

    /**
     * Appends a serialized event atomically within this process.
     *
     * @param output JSONL output path
     * @param event serialized event
     * @throws IOException if the event cannot be appended
     */
    private static void append(Path output, String event) throws IOException {
        synchronized (FILE_LOCK) {
            Path parent = output.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(output, event + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
    }
}
