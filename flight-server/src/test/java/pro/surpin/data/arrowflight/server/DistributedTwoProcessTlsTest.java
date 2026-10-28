package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.services.InternalCallIdentity;

import java.io.File;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Distributed complete mode between two server processes (item 18, D2 (c)): each node is a
 * separate JVM running {@link HadoopArrowFlightServer} with TLS and the token provider, so the
 * node-to-node DoGets go through the real TLS transport, the peer trust of the node's own
 * certificate and the internal identity check in front of the token authenticator. The nodes
 * read one local directory declared shared (dataDirShared). Answers must equal DuckDB's over
 * the same files, and the planning node's log must show they were merged from both nodes.
 */
@Tag("integration")
class DistributedTwoProcessTlsTest {

    private static final int FILES = 4;
    private static final int ROWS = 20_000;
    private static final long START_TIMEOUT_SECONDS = 120;

    @TempDir
    static Path dir;
    private static Path table;
    private static Path certificate;
    private static String token;
    private static final List<Node> NODES = new ArrayList<>();
    private static BufferAllocator allocator;

    /** A server process and the log it wrote so far. */
    private record Node(Process process, int flightPort, Path log) {

        /**
         * Reads the process's output so far.
         *
         * @return log text
         * @throws Exception when it cannot be read
         */
        String output() throws Exception {
            return Files.readString(log, StandardCharsets.UTF_8);
        }
    }

    /**
     * Writes the table, a certificate for localhost and starts two TLS nodes with the token
     * provider on one cluster.
     *
     * @throws Exception on failure
     */
    @BeforeAll
    static void startNodes() throws Exception {
        Path data = Files.createDirectories(dir.resolve("data"));
        table = Files.createDirectories(data.resolve("s").resolve("t"));
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement()) {
            for (int f = 0; f < FILES; f++) {
                statement.execute("COPY (SELECT (i % 10)::INTEGER AS k, (i * " + FILES + " + " + f
                        + ")::BIGINT AS v, 'n' || (i % 13) AS name FROM range(" + ROWS + ") r(i))"
                        + " TO '" + table.resolve("part-" + f + ".parquet") + "' (FORMAT PARQUET)");
            }
        }
        List<Path> pem = selfSignedPem(dir);
        certificate = pem.get(0);
        byte[] secret = new byte[16];
        new SecureRandom().nextBytes(secret);
        token = HexFormat.of().formatHex(secret);

        int[] hazelcastPorts = {freePort(), freePort()};
        String hosts = "127.0.0.1:" + hazelcastPorts[0] + ",127.0.0.1:" + hazelcastPorts[1];
        for (int i = 0; i < 2; i++) {
            NODES.add(start(i, data, freePort(), hazelcastPorts[i], hosts, pem));
        }
        allocator = new RootAllocator(Long.MAX_VALUE);
        for (Node node : NODES) {
            awaitStarted(node);
        }
    }

    /**
     * Stops the server processes.
     *
     * @throws Exception on failure
     */
    @AfterAll
    static void stopNodes() throws Exception {
        for (Node node : NODES) {
            node.process().destroy();
        }
        for (Node node : NODES) {
            if (!node.process().waitFor(30, TimeUnit.SECONDS)) {
                node.process().destroyForcibly();
            }
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    /**
     * Verifies complete answers over TLS with a token are merged from both processes and equal
     * DuckDB's answers over the same files.
     *
     * @throws Exception on failure
     */
    @Test
    void mergedAnswersOverTlsWithTokenEqualDuckDb() throws Exception {
        List<String> queries = List.of(
                "SELECT count(*) AS n, sum(v) AS total, max(name) AS hi FROM s.t",
                "SELECT k, count(*) AS n, sum(v) AS total FROM s.t GROUP BY k ORDER BY k",
                "SELECT v, name FROM s.t WHERE v % 7 = 0 ORDER BY v DESC LIMIT 5",
                "SELECT DISTINCT name FROM s.t ORDER BY name");
        try (FlightClient client = client(); FlightSqlClient sql = new FlightSqlClient(client)) {
            for (String query : queries) {
                assertEquals(duckDb(query), answer(sql, query), query);
            }
        }
        String log = NODES.get(0).output();
        assertEquals(queries.size(), count(log, "distributedComplete=merged endpoints=4"), log);
        assertEquals(0, count(log, "distributedComplete=fallback"), log);
        assertEquals(0, count(log, "distributedComplete=failover"), log);
    }

    /**
     * Verifies the nodes refuse a client without the token and a forged internal identity sent
     * from a member's address.
     *
     * @throws Exception on failure
     */
    @Test
    void callsWithoutTokenOrWithForgedInternalIdentityAreRefused() throws Exception {
        Location location = Location.forGrpcTls("localhost", NODES.get(0).flightPort());
        try (InputStream trusted = Files.newInputStream(certificate);
                FlightClient client = FlightClient.builder(allocator, location)
                        .trustedCertificates(trusted).build();
                FlightSqlClient sql = new FlightSqlClient(client)) {
            FlightRuntimeException noToken = assertThrows(FlightRuntimeException.class,
                    () -> sql.execute("SELECT count(*) FROM s.t"));
            assertEquals(CallStatus.UNAUTHENTICATED.code(), noToken.status().code());

            CallHeaders forged = new FlightCallHeaders();
            forged.insert(InternalCallIdentity.HEADER, "admin:" + Long.MAX_VALUE + ":AAAA");
            FlightRuntimeException forgedIdentity = assertThrows(FlightRuntimeException.class,
                    () -> sql.execute("SELECT count(*) FROM s.t", new HeaderCallOption(forged)));
            assertEquals(CallStatus.UNAUTHENTICATED.code(), forgedIdentity.status().code());
        }
        assertTrue(NODES.get(0).output().contains("internalCall=refused"));
    }

    /**
     * Opens a TLS client of the first node trusting the test certificate.
     *
     * @return client
     * @throws Exception when the certificate cannot be read
     */
    private static FlightClient client() throws Exception {
        try (InputStream trusted = Files.newInputStream(certificate)) {
            return FlightClient.builder(allocator,
                    Location.forGrpcTls("localhost", NODES.get(0).flightPort()))
                    .trustedCertificates(trusted).build();
        }
    }

    /**
     * Runs a query with the token and renders the complete answer.
     *
     * @param sql Flight SQL client
     * @param query SQL
     * @return rows as text, one line per row
     * @throws Exception on failure
     */
    private static String answer(FlightSqlClient sql, String query) throws Exception {
        CallHeaders headers = new FlightCallHeaders();
        headers.insert("authorization", "Bearer " + token);
        HeaderCallOption auth = new HeaderCallOption(headers);
        FlightInfo info = sql.execute(query, auth);
        assertEquals(1, info.getEndpoints().size(), "complete answer: " + query);
        StringBuilder rows = new StringBuilder();
        try (FlightStream stream = sql.getStream(info.getEndpoints().get(0).getTicket(), auth)) {
            while (stream.next()) {
                var root = stream.getRoot();
                for (int r = 0; r < root.getRowCount(); r++) {
                    for (int c = 0; c < root.getFieldVectors().size(); c++) {
                        rows.append(c == 0 ? "" : "|").append(root.getVector(c).getObject(r));
                    }
                    rows.append('\n');
                }
            }
        }
        return rows.toString();
    }

    /**
     * Renders DuckDB's answer of a query over the table's files.
     *
     * @param query SQL naming the table s.t
     * @return rows as text, one line per row
     * @throws Exception on failure
     */
    private static String duckDb(String query) throws Exception {
        String direct = query.replace("s.t", "read_parquet('" + table.resolve("*.parquet") + "')");
        StringBuilder rows = new StringBuilder();
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
                Statement statement = duck.createStatement();
                ResultSet result = statement.executeQuery(direct)) {
            int columns = result.getMetaData().getColumnCount();
            while (result.next()) {
                for (int c = 1; c <= columns; c++) {
                    rows.append(c == 1 ? "" : "|").append(result.getObject(c));
                }
                rows.append('\n');
            }
        }
        return rows.toString();
    }

    /**
     * Starts one node process.
     *
     * @param index node index
     * @param data data directory
     * @param flightPort Flight port
     * @param hazelcastPort Hazelcast port
     * @param hosts Hazelcast seeds of both nodes
     * @param pem certificate and private key
     * @return node
     * @throws Exception on failure
     */
    private static Node start(int index, Path data, int flightPort, int hazelcastPort, String hosts,
            List<Path> pem) throws Exception {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        Path log = dir.resolve("node-" + index + ".log");
        ProcessBuilder builder = new ProcessBuilder(java,
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                "-Xmx512m", "-DlogLevel=info", "-DnumServers=2", "-DresultMode=complete",
                "-DendpointsPerServer=2", "-DdistributedComplete=true", "-DdataDirShared=true",
                "-DduckDbThreads=1", "-DduckDbWarmConnections=1",
                "-cp", System.getProperty("java.class.path"),
                HadoopArrowFlightServer.class.getName(),
                "--data-dir", data.toString(), "--port", String.valueOf(flightPort),
                "--hazelcast-port", String.valueOf(hazelcastPort), "--hosts", hosts,
                "--localhost", "localhost", "--metrics-enabled", "false")
                .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("FLIGHT_TLS_CERT", pem.get(0).toString());
        builder.environment().put("FLIGHT_TLS_KEY", pem.get(1).toString());
        builder.environment().put("FLIGHT_AUTH_PROVIDER", "token");
        builder.environment().put("FLIGHT_AUTH_TOKEN", token);
        return new Node(builder.start(), flightPort, log);
    }

    /**
     * Waits until a node logs that it serves.
     *
     * @param node node
     * @throws Exception when it exits or does not start in time
     */
    private static void awaitStarted(Node node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(START_TIMEOUT_SECONDS);
        while (!node.output().contains("Server Arrow Flight SQL started")) {
            if (!node.process().isAlive() || System.nanoTime() > deadline) {
                throw new IllegalStateException("node did not start:\n" + node.output());
            }
            Thread.sleep(200);
        }
    }

    /**
     * Writes a self-signed certificate for localhost and 127.0.0.1 and its private key as PEM.
     *
     * @param target directory
     * @return certificate file and key file
     * @throws Exception on failure
     */
    private static List<Path> selfSignedPem(Path target) throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        X500Name name = new X500Name("CN=localhost");
        Instant now = Instant.now();
        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(name,
                BigInteger.valueOf(now.toEpochMilli()), Date.from(now.minus(1, ChronoUnit.HOURS)),
                Date.from(now.plus(1, ChronoUnit.DAYS)), name, keys.getPublic());
        builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(
                new GeneralName[] {new GeneralName(GeneralName.dNSName, "localhost"),
                    new GeneralName(GeneralName.iPAddress, "127.0.0.1")}));
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate())));
        Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
        Path certFile = Files.writeString(target.resolve("node.crt"), "-----BEGIN CERTIFICATE-----\n"
                + encoder.encodeToString(cert.getEncoded()) + "\n-----END CERTIFICATE-----\n");
        Path keyFile = Files.writeString(target.resolve("node.key"), "-----BEGIN PRIVATE KEY-----\n"
                + encoder.encodeToString(keys.getPrivate().getEncoded()) + "\n-----END PRIVATE KEY-----\n");
        return List.of(certFile, keyFile);
    }

    /**
     * Counts the occurrences of a text.
     *
     * @param text text
     * @param part searched part
     * @return occurrences
     */
    private static int count(String text, String part) {
        int n = 0;
        for (int i = text.indexOf(part); i >= 0; i = text.indexOf(part, i + part.length())) {
            n++;
        }
        return n;
    }

    /**
     * Finds a free TCP port.
     *
     * @return port
     * @throws Exception on failure
     */
    private static int freePort() throws Exception {
        return TestPorts.free();
    }
}
