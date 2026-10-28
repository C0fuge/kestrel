package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.auth.ldap.LdapTestAccess;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Logs ClickHouse's arrowFlight table function in through the LDAP provider over TLS: its Basic
 * handshake gets the session bearer, which it then sends on GetSchema, GetFlightInfo and DoGet.
 * Runs {@code clickhouse local} in Docker; needs {@code -Dclickhouse.it.image}, skipped without it.
 */
@Tag("integration")
class ClickHouseLdapLoginTest {

    private static final String CLICKHOUSE = System.getProperty("clickhouse.it.image", "");
    private static final Schema SCHEMA = new Schema(List.of(
            Field.nullable("user", new ArrowType.Utf8()), Field.nullable("n", new ArrowType.Int(64, true))));

    @TempDir
    static Path dir;
    private static LdapTestAccess ldap;
    private static BufferAllocator allocator;
    private static FlightSecurity security;
    private static FlightServer server;
    private static Path certificates;
    /** Call name and peer identity of every call the producer served. */
    private static final List<String> CALLS = Collections.synchronizedList(new ArrayList<>());

    @BeforeAll
    static void start() throws Exception {
        assumeTrue(!CLICKHOUSE.isBlank(), "needs -Dclickhouse.it.image and Docker");
        ldap = LdapTestAccess.start(dir);
        List<Path> pem = ldap.pem(dir);
        certificates = Files.createDirectories(dir.resolve("certs"));
        Files.copy(pem.get(0), certificates.resolve("ca.pem"));
        // The container's clickhouse user must read the mounted certificate.
        Files.setPosixFilePermissions(certificates, PosixFilePermissions.fromString("rwxr-xr-x"));
        Files.setPosixFilePermissions(certificates.resolve("ca.pem"), PosixFilePermissions.fromString("rw-r--r--"));
        Map<String, String> settings = ldap.templateSettings();
        settings.put("FLIGHT_AUTH_PROVIDER", "ldap");
        settings.put("FLIGHT_TLS_CERT", pem.get(0).toString());
        settings.put("FLIGHT_TLS_KEY", pem.get(1).toString());
        security = FlightSecurity.fromSettings(settings);
        allocator = new RootAllocator();
        server = security.configure(FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0),
                new NoOpFlightProducer() {
                    @Override
                    public SchemaResult getSchema(CallContext context, FlightDescriptor descriptor) {
                        CALLS.add("schema " + context.peerIdentity() + " " + descriptor.getPath());
                        return new SchemaResult(SCHEMA);
                    }

                    @Override
                    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                        CALLS.add("info " + context.peerIdentity() + " " + descriptor.getPath());
                        return new FlightInfo(SCHEMA, descriptor,
                                List.of(new FlightEndpoint(new Ticket(new byte[] {1}))), -1, -1);
                    }

                    @Override
                    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                        CALLS.add("get " + context.peerIdentity());
                        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                            ((VarCharVector) root.getVector(0)).setSafe(0,
                                    context.peerIdentity().getBytes(StandardCharsets.UTF_8));
                            ((BigIntVector) root.getVector(1)).setSafe(0, 42);
                            root.setRowCount(1);
                            listener.start(root);
                            listener.putNext();
                            listener.completed();
                        }
                    }
                })).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
            security.close();
            allocator.close();
            ldap.close();
        }
    }

    /**
     * Reads the producer's table as alice: every call carries her session; a wrong password
     * fails in ClickHouse's handshake before any call reaches the producer.
     *
     * @throws Exception on Docker failure
     */
    @Test
    void clickhouseLogsInWithBasicThenUsesTheSession() throws Exception {
        CALLS.clear();
        Result ok = run(query("alice-pw"));
        assertEquals(0, ok.exit(), ok.output());
        assertEquals("alice\t42", lastLine(ok.output()), ok.output());
        assertTrue(CALLS.contains("schema alice [sales.orders]"), CALLS.toString());
        assertTrue(CALLS.contains("info alice [sales.orders]"), CALLS.toString());
        assertTrue(CALLS.contains("get alice"), CALLS.toString());

        CALLS.clear();
        Result denied = run(query("wrong"));
        assertNotEquals(0, denied.exit(), denied.output());
        assertTrue(denied.output().contains("authenticate"), denied.output());
        assertEquals(List.of(), CALLS);
    }

    /**
     * Builds the clickhouse local command reading the table with a named collection over TLS.
     *
     * @param password alice's password to send
     * @return command and arguments
     */
    private static List<String> query(String password) {
        String sql = "CREATE NAMED COLLECTION flight AS host = 'localhost', port = " + server.getPort()
                + ", dataset = 'sales.orders', use_basic_authentication = true, username = 'alice',"
                + " password = '" + password + "', enable_ssl = true, ssl_ca = '/certs/ca.pem';"
                + " SELECT user, n FROM arrowFlight(flight) FORMAT TSV";
        return List.of("docker", "run", "--rm", "--network", "host", "-v", certificates + ":/certs:ro",
                CLICKHOUSE, "clickhouse", "local", "--query", sql);
    }

    /**
     * Returns the last non-empty line of an output.
     *
     * @param text process output
     * @return last line
     */
    private static String lastLine(String text) {
        List<String> lines = text.lines().filter(l -> !l.isBlank()).toList();
        return lines.isEmpty() ? "" : lines.get(lines.size() - 1).strip();
    }

    /**
     * Runs a process.
     *
     * @param command command and arguments
     * @return exit code and combined output
     * @throws IOException on start failure
     * @throws InterruptedException when interrupted
     */
    private static Result run(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output;
        try (InputStream in = process.getInputStream()) {
            output = in.readAllBytes();
        }
        boolean done = process.waitFor(10, TimeUnit.MINUTES);
        String text = new String(output, StandardCharsets.UTF_8);
        if (!done) {
            process.destroyForcibly();
        }
        assertTrue(done, "clickhouse timed out:\n" + text);
        return new Result(process.exitValue(), text);
    }

    /**
     * Outcome of a process.
     *
     * @param exit exit code
     * @param output combined standard output and error
     */
    private record Result(int exit, String output) {
    }
}
