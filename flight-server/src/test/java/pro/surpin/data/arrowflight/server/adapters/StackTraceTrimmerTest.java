package pro.surpin.data.arrowflight.server.adapters;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.function.Consumer;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests that clients get error texts without Java stack frames. */
class StackTraceTrimmerTest {

    /** First line of the NameNode error the secure HA stand showed. */
    private static final String DENIED = "IO Error: Permission denied: user=flight, access=READ_EXECUTE,"
            + " inode=\"/data/tpch\":hdfs:supergroup:drwx------";
    /** The NameNode's stack trace that followed it (Hadoop IPC sends the stringified exception). */
    private static final String NAMENODE_TRACE = "\n\tat org.apache.hadoop.hdfs.server.namenode"
            + ".FSPermissionChecker.check(FSPermissionChecker.java:399)"
            + "\n\tat org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker.checkPermission("
            + "FSPermissionChecker.java:255)"
            + "\n\tat org.apache.hadoop.ipc.Server$Handler$1.run(Server.java:2682)"
            + "\n\tat java.security.AccessController.doPrivileged(Native Method)"
            + "\n\tat java.base/java.lang.Thread.run(Thread.java:840)"
            + "\n\tat org.apache.hadoop.ipc.ProtobufRpcEngine$Server$ProtoBufRpcInvoker.<init>"
            + "(ProtobufRpcEngine.java:1)"
            + "\nCaused by: java.io.IOException: inner"
            + "\n\tat app//org.apache.hadoop.Foo.bar(Foo.java:12)"
            + "\n\t... 12 more\n";

    /** Verifies stack frames and "... n more" lines go, the message and its causes stay. */
    @Tag("unit")
    @Test
    void removesStackFrames() {
        assertEquals(DENIED + "\nCaused by: java.io.IOException: inner",
                StackTraceTrimmer.trim(DENIED + NAMENODE_TRACE));
        assertEquals("x", StackTraceTrimmer.trim("x\r\n\tat a.B.c(B.java:1)\r\n"));
    }

    /** Verifies texts without stack frames are returned unchanged, multi-line ones included. */
    @Tag("unit")
    @Test
    void keepsTextsWithoutFrames() {
        String binder = "Binder Error: Referenced column \"x\" not found\n\nLINE 1: SELECT x FROM t\n"
                + "               ^";
        assertSame(binder, StackTraceTrimmer.trim(binder));
        String sql = "Parser Error: syntax error at or near \"at\"\nLINE 1: select at foo(1)";
        assertSame(sql, StackTraceTrimmer.trim(sql));
        assertSame(DENIED, StackTraceTrimmer.trim(DENIED));
        assertNull(StackTraceTrimmer.trim(null));
    }

    /** Verifies the query failure text drops the frames before it is cut to its length limit. */
    @Tag("unit")
    @Test
    void failureDescriptionHasNoFrames() {
        StringBuilder trace = new StringBuilder(DENIED);
        for (int i = 0; i < 40; i++) {
            trace.append("\n\tat org.apache.hadoop.ipc.Server$Handler.run(Server.java:").append(i).append(')');
        }
        assertEquals("Query execution failed: SQLException: " + DENIED,
                FlightSqlProducer.failureDescription(new RuntimeException("wrapped",
                        new java.sql.SQLException(trace.toString()))));
    }

    /** Verifies the interceptor rewrites a closing status with frames and passes others through. */
    @Tag("unit")
    @Test
    @SuppressWarnings("unchecked")
    void interceptorTrimsTheClosingStatus() {
        ServerCall<Object, Object> call = mock(ServerCall.class);
        ServerCallHandler<Object, Object> next = mock(ServerCallHandler.class);
        ArgumentCaptor<ServerCall<Object, Object>> wrapped = ArgumentCaptor.forClass(ServerCall.class);
        when(next.startCall(wrapped.capture(), any())).thenReturn(mock(ServerCall.Listener.class));
        new StackTraceTrimmer().interceptCall(call, new Metadata(), next);

        Metadata trailers = new Metadata();
        wrapped.getValue().close(Status.INTERNAL.withDescription(DENIED + NAMENODE_TRACE), trailers);
        ArgumentCaptor<Status> closed = ArgumentCaptor.forClass(Status.class);
        verify(call).close(closed.capture(), same(trailers));
        assertEquals(Status.Code.INTERNAL, closed.getValue().getCode());
        assertEquals(DENIED + "\nCaused by: java.io.IOException: inner", closed.getValue().getDescription());

        Status plain = Status.NOT_FOUND.withDescription("no such table");
        wrapped.getValue().close(plain, trailers);
        verify(call).close(same(plain), same(trailers));
    }

    /** Verifies a Flight client sees the trimmed text of a server error, as on the stand. */
    @Tag("unit")
    @Test
    void flightClientGetsTrimmedError() throws Exception {
        FlightProducer producer = new NoOpFlightProducer() {
            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                listener.onError(CallStatus.INTERNAL.withDescription(DENIED + NAMENODE_TRACE)
                        .toRuntimeException());
            }
        };
        StackTraceTrimmer trimmer = new StackTraceTrimmer();
        try (BufferAllocator allocator = new RootAllocator();
                FlightServer server = FlightServer.builder(allocator,
                        Location.forGrpcInsecure("127.0.0.1", 0), producer)
                        .transportHint("grpc.builderConsumer",
                                (Consumer<io.grpc.netty.NettyServerBuilder>) b -> b.intercept(trimmer))
                        .build().start();
                FlightClient client = FlightClient.builder(allocator, server.getLocation()).build()) {
            FlightRuntimeException failure = assertThrows(FlightRuntimeException.class,
                    () -> client.doAction(new Action("x")).hasNext());
            assertEquals(DENIED + "\nCaused by: java.io.IOException: inner", failure.getMessage());
            assertFalse(failure.getMessage().contains("FSPermissionChecker"));
            assertTrue(failure.getMessage().startsWith("IO Error: Permission denied"));
        }
    }
}
