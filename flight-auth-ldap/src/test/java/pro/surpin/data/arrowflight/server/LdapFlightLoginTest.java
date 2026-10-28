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
import pro.surpin.data.arrowflight.auth.ldap.LdapTestAccess;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Logs in through a real TLS Flight server selected with FLIGHT_AUTH_PROVIDER=ldap. */
@Tag("unit")
class LdapFlightLoginTest {
    @TempDir
    static Path dir;
    private static LdapTestAccess ldap;
    private static BufferAllocator allocator;
    private static FlightSecurity security;
    private static FlightServer server;
    private static Path certificate;

    @BeforeAll
    static void start() throws Exception {
        ldap = LdapTestAccess.start(dir);
        List<Path> pem = ldap.pem(dir);
        certificate = pem.get(0);
        Map<String, String> settings = ldap.templateSettings();
        settings.put("FLIGHT_AUTH_PROVIDER", "ldap");
        settings.put("FLIGHT_LDAP_GROUP_SEARCH_BASE", "ou=groups,dc=example,dc=org");
        settings.put("FLIGHT_TLS_CERT", pem.get(0).toString());
        settings.put("FLIGHT_TLS_KEY", pem.get(1).toString());
        security = FlightSecurity.fromSettings(settings);
        allocator = new RootAllocator();
        server = security.configure(FlightServer.builder(allocator, Location.forGrpcTls("localhost", 0),
                new NoOpFlightProducer() {
                    @Override
                    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
                        listener.onNext(new Result(context.peerIdentity().getBytes(StandardCharsets.UTF_8)));
                        listener.onCompleted();
                    }
                })).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
        security.close();
        allocator.close();
        ldap.close();
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
     * @return peer identity the server saw
     */
    private static String whoAmI(FlightClient client, CredentialCallOption credential) {
        Iterator<Result> results = client.doAction(new Action("whoami"), credential);
        return new String(results.next().getBody(), StandardCharsets.UTF_8);
    }

    /** Basic login returns a session bearer token; later calls use it and see the LDAP user. */
    @Test
    void basicLoginThenSessionCalls() throws Exception {
        try (FlightClient client = client()) {
            CredentialCallOption session = client.authenticateBasicToken("alice", "alice-pw").orElseThrow();
            assertEquals("alice", whoAmI(client, session));
            assertEquals("alice", whoAmI(client, session));
        }
        try (FlightClient client = client()) {
            CredentialCallOption session = client.authenticateBasicToken("bob", "bob:pw:with:colons")
                    .orElseThrow();
            assertEquals("bob", whoAmI(client, session));
        }
    }

    /** Wrong passwords, empty passwords, forged sessions and missing credentials are UNAUTHENTICATED. */
    @Test
    void refusesBadCredentials() throws Exception {
        try (FlightClient client = client()) {
            for (String[] login : new String[][] {{"alice", "wrong"}, {"alice", ""}, {"nobody", "x"}}) {
                FlightRuntimeException e = assertThrows(FlightRuntimeException.class,
                        () -> client.authenticateBasicToken(login[0], login[1]));
                assertEquals(FlightStatusCode.UNAUTHENTICATED, e.status().code());
            }
            CredentialCallOption session = client.authenticateBasicToken("alice", "alice-pw").orElseThrow();
            CredentialCallOption forged = new CredentialCallOption(
                    new BearerCredentialWriter("eA.eQ"));
            assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows(FlightRuntimeException.class,
                    () -> whoAmI(client, forged)).status().code());
            assertEquals(FlightStatusCode.UNAUTHENTICATED, assertThrows(FlightRuntimeException.class,
                    () -> client.doAction(new Action("whoami")).next()).status().code());
            assertEquals("alice", whoAmI(client, session));
        }
    }

    /** The provider is found by name, establishes identities and refuses to run without TLS. */
    @Test
    void providerSelectionAndTlsRequirement() {
        assertTrue(security.authentication().establishesIdentity());
        Map<String, String> settings = ldap.templateSettings();
        settings.put("FLIGHT_AUTH_PROVIDER", "ldap");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> FlightSecurity.fromSettings(settings));
        assertTrue(e.getMessage().contains("requires TLS"), e.getMessage());
        settings.put("FLIGHT_BEARER_TOKEN", "x");
        assertThrows(IllegalArgumentException.class, () -> FlightSecurity.fromSettings(settings));
    }

    /** The provider's groups reach the Ranger group set, next to the Hadoop group mapping. */
    @Test
    void directoryGroupsReachPolicyChecks() throws Exception {
        try (FlightClient client = client()) {
            CredentialCallOption session = client.authenticateBasicToken("bob", "bob:pw:with:colons")
                    .orElseThrow();
            assertEquals("bob", whoAmI(client, session));
        }
        assertEquals(Set.of("bi-users", "ops"), security.authentication().groups("bob"));
        assertEquals(Set.of("analysts", "bi-users"), security.authentication().groups("alice"));
        assertEquals(Set.of("bi-users", "ops", "staff"), RangerAuthorization.union(
                security.authentication()::groups, identity -> Set.of("staff")).apply("bob"));
    }
}
