package pro.surpin.data.arrowflight.plugin.lakefs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The REST call against a fake lakeFS API. */
class HttpRefResolverTest {

    private HttpServer server;
    private final List<String> paths = new CopyOnWriteArrayList<>();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/api/v1/", exchange -> {
            String path = exchange.getRequestURI().getRawPath();
            paths.add(path);
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            int status;
            String body;
            if (path.equals("/api/v1/repositories/lake/branches/main")) {
                status = 200;
                body = "{\"id\":\"main\",\"commit_id\":\"c0ffee42\"}";
            } else if (path.equals("/api/v1/repositories/lake/branches/broken")) {
                status = 200;
                body = "{\"id\":\"broken\"}";
            } else if (path.equals("/api/v1/repositories/lake/commits/v1")) {
                status = 200;
                body = "{\"committer\":\"admin\",\"meta_range_id\":\"m1\",\"id\":\"c0ffee43\","
                        + "\"parents\":[\"c0ffee42\"]}";
            } else if (path.equals("/api/v1/repositories/lake")) {
                status = 200;
                body = "{\"id\":\"lake\"}";
            } else if (path.startsWith("/api/v1/repositories/locked/")) {
                status = 401;
                body = "{\"message\":\"secret-in-body\"}";
            } else {
                status = 404;
                body = "{\"message\":\"not found\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private HttpRefResolver resolver() {
        return new HttpRefResolver(URI.create("http://127.0.0.1:" + server.getAddress().getPort()
                + "/api/v1"), "AKIALAKEFS", "s3cr3t", null, Duration.ofSeconds(5));
    }

    @Test
    void branchResolvesToItsHeadCommitWithBasicAuth() throws IOException {
        assertEquals(Optional.of("c0ffee42"), resolver().branchHead("lake", "main"));
        assertEquals(List.of("/api/v1/repositories/lake/branches/main"), paths);
        assertEquals("Basic " + Base64.getEncoder().encodeToString(
                "AKIALAKEFS:s3cr3t".getBytes(StandardCharsets.UTF_8)), authorizations.get(0));
    }

    @Test
    void commitIdOrTagIsNotABranch() throws IOException {
        assertTrue(resolver().branchHead("lake", "0123abcd").isEmpty());
    }

    @Test
    void errorsNameTheStatusButNeitherBodyNorSecret() {
        IOException e = assertThrows(IOException.class,
                () -> resolver().branchHead("locked", "main"));
        assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
        assertFalse(e.getMessage().contains("secret"), e.getMessage());
        assertFalse(e.getMessage().contains("s3cr3t"), e.getMessage());
        assertThrows(IOException.class, () -> resolver().branchHead("lake", "broken"));
        assertTrue(e.getMessage().contains("refused the credentials"), e.getMessage());
    }

    @Test
    void tagOrCommitIsConfirmedAndUnknownRefsAreNamed() throws IOException {
        assertEquals("c0ffee43", resolver().refCommit("lake", "v1"));
        IOException ref = assertThrows(IOException.class,
                () -> resolver().refCommit("lake", "mian"));
        assertEquals("lakeFS repository 'lake' has no branch, tag or commit 'mian'",
                ref.getMessage());
        IOException repo = assertThrows(IOException.class,
                () -> resolver().refCommit("nosuch", "main"));
        assertTrue(repo.getMessage().startsWith("lakeFS repository 'nosuch' does not exist"),
                repo.getMessage());
    }

    @Test
    void unreachableLakeFsIsNamed() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        HttpRefResolver closed = new HttpRefResolver(URI.create("http://127.0.0.1:" + port
                + "/api/v1"), "AKIALAKEFS", "s3cr3t", null, Duration.ofSeconds(2));
        IOException e = assertThrows(IOException.class, () -> closed.branchHead("lake", "main"));
        assertTrue(e.getMessage().contains("lakeFS not reachable: ConnectException"),
                e.getMessage());
        assertFalse(e.getMessage().contains("s3cr3t"), e.getMessage());
    }
}
