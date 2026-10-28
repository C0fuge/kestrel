package pro.surpin.data.arrowflight.server;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.data.arrowflight.auth.jwt.JwtTestIssuer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Calls a real TLS Flight server selected with FLIGHT_AUTH_PROVIDER=jwt. */
@Tag("unit")
class JwtFlightLoginTest {
    @TempDir
    static Path dir;
    private static JwtTestIssuer issuer;
    private static BufferAllocator allocator;
    private static FlightSecurity security;
    private static FlightServer server;
    private static Path certificate;

    @BeforeAll
    static void start() throws Exception {
        issuer = JwtTestIssuer.create(dir);
        List<Path> pem = issuer.pem();
        certificate = pem.get(0);
        Map<String, String> settings = issuer.fileSettings();
        settings.put("FLIGHT_AUTH_PROVIDER", "jwt");
        settings.put("FLIGHT_JWT_GROUPS_CLAIM", "groups");
        settings.put("FLIGHT_JWT_BASIC_PASSWORD", "true");
        settings.put("FLIGHT_TLS_CERT", pem.get(0).toString());
        settings.put("FLIGHT_TLS_KEY", pem.get(1).toString());
        security = FlightSecurity.fromSettings(settings);
        allocator = new RootAllocator();
        server = security.configure(FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0),
                new NoOpFlightProducer() {
                    @Override
                    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                        String identity = context.peerIdentity();
                        listener.onNext(new Result((identity + " " + new java.util.TreeSet<>(
                                security.authentication().groups(identity))).getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                    }
                })).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
        security.close();
        allocator.close();
        issuer.close();
    }

    /**
     * Opens a TLS client trusting the test certificate.
     *
     * @return client
     * @throws Exception when the certificate cannot be read
     */
    private static FlightClient client() throws Exception {
        try (InputStream trusted = Files.newInputStream(certificate)) {
            return FlightClient.builder(allocator, Location.forGrpcTls("localhost", server.getPort()))
                    .trustedCertificates(trusted).build();
        }
    }

    /**
     * Runs the identity action.
     *
     * @param client client
     * @param credential call credential
     * @return identity and groups the server saw
     */
    private static String whoAmI(FlightClient client, CredentialCallOption credential) {
        Iterator<Result> results = client.doAction(new Action("whoami"), credential);
        return new String(results.next().getBody(), StandardCharsets.UTF_8);
    }

    /**
     * Wraps a token as bearer credential.
     *
     * @param token token
     * @return call option
     */
    private static CredentialCallOption bearer(String token) {
        return new CredentialCallOption(new BearerCredentialWriter(token));
    }

    /** Each call carries the JWT; the server sees its subject and the groups of the call's own token. */
    @Test
    void bearerCallsCarryIdentityAndGroups() throws Exception {
        try (FlightClient client = client()) {
            String alice = issuer.token("alice", b -> b.claim("groups", List.of("analysts", "bi-users")));
            assertEquals("alice [analysts, bi-users]", whoAmI(client, bearer(alice)));
            assertEquals("alice [analysts, bi-users]", whoAmI(client, bearer(alice)));
            assertEquals("bob [ops]", whoAmI(client, bearer(issuer.token("bob", b -> b.claim("groups", "ops")))));
            assertEquals("alice []", whoAmI(client, bearer(issuer.token("alice", b -> { }))));
        }
        assertEquals(Set.of("ops", "staff"), RangerAuthorization.union(
                security.authentication()::groups, identity -> Set.of("staff")).apply("bob"));
        assertThrows(IllegalStateException.class, () -> security.authentication().groups("never-seen"));
    }

    /** With FLIGHT_JWT_BASIC_PASSWORD a Basic login whose password is the token gets it back as bearer. */
    @Test
    void basicPasswordCarriesToken() throws Exception {
        String token = issuer.token("carol", b -> b.claim("groups", List.of("bi-users")));
        try (FlightClient client = client()) {
            CredentialCallOption session = client.authenticateBasicToken("carol", token).orElseThrow();
            assertEquals("carol [bi-users]", whoAmI(client, session));
            assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows(FlightRuntimeException.class,
                    () -> client.authenticateBasicToken("carol", "password")).status().code());
        }
    }

    /** Expired, foreign, forged and missing credentials are UNAUTHENTICATED. */
    @Test
    void refusesBadTokens() throws Exception {
        try (FlightClient client = client()) {
            for (String token : new String[] {
                issuer.token("alice", b -> b.expirationTime(Date.from(Instant.now().minusSeconds(3600)))),
                issuer.token("alice", b -> b.audience("other")),
                issuer.token("alice", b -> b.issuer("https://evil.test")),
                "eA.eQ.eA",
            }) {
                assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows(FlightRuntimeException.class,
                        () -> whoAmI(client, bearer(token))).status().code());
            }
            assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows(FlightRuntimeException.class,
                    () -> client.doAction(new Action("whoami")).next()).status().code());
        }
    }

    /** The provider is found by name, establishes identities and refuses to run without TLS. */
    @Test
    void providerSelectionAndTlsRequirement() throws Exception {
        assertTrue(security.authentication().establishesIdentity());
        Map<String, String> settings = issuer.fileSettings();
        settings.put("FLIGHT_AUTH_PROVIDER", "jwt");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(settings));
        assertTrue(e.getMessage().contains("requires TLS"), e.getMessage());
        settings.put("FLIGHT_BEARER_TOKEN", "x");
        assertThrows(IllegalArgumentException.class, () -> FlightSecurity.fromSettings(settings));
    }
}
