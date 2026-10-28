package pro.surpin.data.arrowflight.auth.jwt;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Admits calls carrying a valid JWT as bearer token (each call carries it; no server session) and,
 * when enabled, a Basic credential whose password is such a token, answered with that token as
 * bearer for the following calls.
 */
final class JwtFlightAuthenticator implements CallHeaderAuthenticator {
    private static final Logger LOGGER = LoggerFactory.getLogger(JwtFlightAuthenticator.class);
    private static final int MAX_HEADER_LENGTH = 16_384;
    private final JwtVerifier verifier;
    private final JwtGroups groups;
    private final boolean basicPassword;

    /**
     * Creates the authenticator.
     *
     * @param verifier token check
     * @param groups group store, filled by each verified token
     * @param basicPassword whether a Basic password may carry the token
     */
    JwtFlightAuthenticator(JwtVerifier verifier, JwtGroups groups, boolean basicPassword) {
        this.verifier = verifier;
        this.groups = groups;
        this.basicPassword = basicPassword;
    }

    @Override
    public AuthResult authenticate(CallHeaders headers) {
        String header = headers.get(Auth2Constants.AUTHORIZATION_HEADER);
        if (header == null || header.length() > MAX_HEADER_LENGTH) {
            throw unauthenticated("JWT bearer token required");
        }
        if (header.startsWith(Auth2Constants.BEARER_PREFIX)) {
            String identity = verify(header.substring(Auth2Constants.BEARER_PREFIX.length()).trim());
            return () -> identity;
        }
        if (!basicPassword || !header.startsWith(Auth2Constants.BASIC_PREFIX)) {
            throw unauthenticated("JWT bearer token required");
        }
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(
                    header.substring(Auth2Constants.BASIC_PREFIX.length()).trim()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw unauthenticated("Malformed Basic credential");
        }
        int colon = decoded.indexOf(':');
        if (colon < 0) {
            throw unauthenticated("Malformed Basic credential");
        }
        String token = decoded.substring(colon + 1).trim();
        String identity = verify(token);
        return new AuthResult() {
            @Override
            public String getPeerIdentity() {
                return identity;
            }

            @Override
            public void appendToOutgoingHeaders(CallHeaders outgoing) {
                outgoing.insert(Auth2Constants.AUTHORIZATION_HEADER, Auth2Constants.BEARER_PREFIX + token);
            }
        };
    }

    /**
     * Verifies a token and remembers its groups for the call's policy checks.
     *
     * @param token compact JWT
     * @return identity
     */
    private String verify(String token) {
        JwtVerifier.Verified verified;
        try {
            verified = verifier.verify(token);
        } catch (JwtVerifier.InvalidTokenException e) {
            LOGGER.info("JWT refused: {}", printable(String.valueOf(e.getMessage())));
            throw unauthenticated("Invalid or expired JWT");
        } catch (JwtVerifier.KeysUnavailableException e) {
            LOGGER.warn("JWT not checked: {}", printable(String.valueOf(e.getMessage())));
            throw CallStatus.UNAVAILABLE.withDescription("JWT signing keys unavailable").toRuntimeException();
        }
        groups.put(verified);
        return verified.identity();
    }

    /**
     * Builds an authentication error without credential details.
     *
     * @param message client-facing reason
     * @return status exception
     */
    private static RuntimeException unauthenticated(String message) {
        return CallStatus.UNAUTHENTICATED.withDescription(message).toRuntimeException();
    }

    /**
     * Shortens a reason for a log line and drops control characters.
     *
     * @param text reason
     * @return loggable text
     */
    private static String printable(String text) {
        String shortened = text.length() > 200 ? text.substring(0, 200) + "..." : text;
        return shortened.replaceAll("\\p{Cntrl}", "?");
    }
}
