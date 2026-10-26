package pro.surpin.data.arrowflight.server.adapters;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Removes Java stack frames from the error text a client receives: storage errors (HDFS
 * NameNode RemoteExceptions, also through DuckDB) carry the remote server's stack trace in their
 * message. The server log keeps the full text.
 */
public final class StackTraceTrimmer implements ServerInterceptor {

    /** One frame of a printed Java stack trace, e.g. {@code \tat java.base/a.B.c(B.java:1)}. */
    private static final Pattern FRAME = Pattern.compile(
            "[ \\t]*at (?:[\\w$.@-]*/)*[\\w$]+(?:\\.[\\w$]+)*\\.[\\w$<>]+\\([^()\\r\\n]*\\)[ \\t]*");
    /** The line a printed stack trace puts for frames it shares with the enclosing trace. */
    private static final Pattern MORE = Pattern.compile(
            "[ \\t]*\\.\\.\\. \\d+ (?:more|common frames omitted)[ \\t]*");

    /**
     * Removes the stack frame lines from an error text.
     *
     * @param text error text, may be null
     * @return the text without stack frame lines, the same instance when it has none
     */
    public static String trim(String text) {
        if (text == null || !text.contains("at ")) {
            return text;
        }
        StringBuilder kept = new StringBuilder(text.length());
        boolean removed = false;
        for (String line : text.split("\\r?\\n", -1)) {
            if (FRAME.matcher(line).matches() || MORE.matcher(line).matches()) {
                removed = true;
                continue;
            }
            if (!kept.isEmpty()) {
                kept.append('\n');
            }
            kept.append(line);
        }
        if (!removed) {
            return text;
        }
        return kept.toString().stripTrailing();
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
            ServerCallHandler<Q, R> next) {
        return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
            @Override
            public void close(Status status, Metadata trailers) {
                String description = status.getDescription();
                String trimmed = trim(description);
                super.close(Objects.equals(trimmed, description) ? status : status.withDescription(trimmed),
                        trailers);
            }
        }, headers);
    }
}
