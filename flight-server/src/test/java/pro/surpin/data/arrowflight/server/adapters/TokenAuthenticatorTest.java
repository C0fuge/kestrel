package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the shared access token check. */
@Tag("unit")
class TokenAuthenticatorTest {

    private final TokenAuthenticator authenticator = new TokenAuthenticator("s3cret");

    /** Verifies the token is accepted as a bearer token. */
    @Test
    void acceptsBearerToken() {
        assertNotNull(authenticator.authenticate(headers("Bearer s3cret")).getPeerIdentity());
    }

    /** Verifies a password login is answered with the bearer token for later calls. */
    @Test
    void acceptsTokenAsBasicPasswordAndReturnsBearer() {
        String basic = Base64.getEncoder().encodeToString(
                "anyone:s3cret".getBytes(StandardCharsets.UTF_8));
        CallHeaderAuthenticator.AuthResult result = authenticator.authenticate(headers("Basic " + basic));
        CallHeaders outgoing = new FlightCallHeaders();

        result.appendToOutgoingHeaders(outgoing);

        assertEquals("Bearer s3cret", outgoing.get(Auth2Constants.AUTHORIZATION_HEADER));
    }

    /** Verifies wrong, malformed and missing credentials are all refused. */
    @Test
    void refusesEverythingElse() {
        String wrongBasic = Base64.getEncoder().encodeToString(
                "anyone:other".getBytes(StandardCharsets.UTF_8));
        for (CallHeaders headers : new CallHeaders[] {
                headers("Bearer other"), headers("Bearer s3cret1"), headers("Basic " + wrongBasic),
                headers("Basic not-base64!"), headers("s3cret"), new FlightCallHeaders()}) {
            FlightRuntimeException error = assertThrows(
                    FlightRuntimeException.class, () -> authenticator.authenticate(headers));
            assertEquals(FlightStatusCode.UNAUTHENTICATED, error.status().code());
        }
    }

    /** Verifies an empty token cannot silently disable the check. */
    @Test
    void rejectsBlankToken() {
        assertThrows(IllegalArgumentException.class, () -> new TokenAuthenticator(" "));
    }

    /**
     * Builds call headers with an authorization value.
     *
     * @param authorization header value
     * @return call headers
     */
    private static CallHeaders headers(String authorization) {
        CallHeaders headers = new FlightCallHeaders();
        headers.insert(Auth2Constants.AUTHORIZATION_HEADER, authorization);
        return headers;
    }
}
