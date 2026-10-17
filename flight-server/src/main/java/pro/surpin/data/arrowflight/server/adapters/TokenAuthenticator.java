package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Admits calls that carry the configured access token, as a bearer token or as a Basic password.
 */
public final class TokenAuthenticator implements CallHeaderAuthenticator {

    private static final String PEER = "token";

    private final byte[] token;

    /**
     * Creates an authenticator for one shared token.
     *
     * @param token access token every call must present
     */
    public TokenAuthenticator(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Access token must not be blank");
        }
        this.token = token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public AuthResult authenticate(CallHeaders incomingHeaders) {
        String header = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
        if (header != null && header.startsWith(Auth2Constants.BEARER_PREFIX)) {
            if (matches(header.substring(Auth2Constants.BEARER_PREFIX.length()))) {
                return () -> PEER;
            }
        } else if (header != null && header.startsWith(Auth2Constants.BASIC_PREFIX)) {
            if (matches(basicPassword(header.substring(Auth2Constants.BASIC_PREFIX.length())))) {
                // Clients that log in with a password expect a bearer token for later calls.
                return new AuthResult() {
                    @Override
                    public String getPeerIdentity() {
                        return PEER;
                    }

                    @Override
                    public void appendToOutgoingHeaders(CallHeaders outgoingHeaders) {
                        outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER,
                                Auth2Constants.BEARER_PREFIX
                                        + new String(token, StandardCharsets.UTF_8));
                    }
                };
            }
        }
        throw CallStatus.UNAUTHENTICATED
                .withDescription("Missing or wrong access token").toRuntimeException();
    }

    /**
     * Compares a presented token with the configured one in constant time.
     *
     * @param presented token sent by the client, or null
     * @return true when it equals the configured token
     */
    private boolean matches(String presented) {
        return presented != null && MessageDigest.isEqual(
                presented.trim().getBytes(StandardCharsets.UTF_8), token);
    }

    /**
     * Extracts the password from a Basic credential.
     *
     * @param encoded Base64 of user name, colon, password
     * @return password, or null when the credential is malformed
     */
    private static String basicPassword(String encoded) {
        try {
            String decoded = new String(
                    Base64.getDecoder().decode(encoded.trim()), StandardCharsets.UTF_8);
            int colon = decoded.indexOf(':');
            return colon < 0 ? null : decoded.substring(colon + 1);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
