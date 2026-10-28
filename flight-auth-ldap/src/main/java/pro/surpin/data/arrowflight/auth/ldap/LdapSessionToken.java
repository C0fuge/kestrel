package pro.surpin.data.arrowflight.auth.ldap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Clock;
import java.util.Base64;

/** Signs the short-lived bearer sessions issued after a successful LDAP bind. */
final class LdapSessionToken {
    private static final String VERSION = "ldap1";
    private static final int MAX_TOKEN_LENGTH = 4096;
    private static final int MAX_USER_LENGTH = 1024;
    private final byte[] secret;
    private final long lifetimeMillis;
    private final Clock clock;

    /**
     * Creates a codec with a key shared by all Flight nodes.
     *
     * @param secret HMAC key with at least 32 bytes
     * @param lifetimeMillis how long an issued token is valid
     * @param clock time source
     */
    LdapSessionToken(byte[] secret, long lifetimeMillis, Clock clock) {
        if (secret == null || secret.length < 32) {
            throw new IllegalArgumentException("LDAP session secret must be at least 32 bytes");
        }
        if (lifetimeMillis <= 0) {
            throw new IllegalArgumentException("LDAP session lifetime must be positive");
        }
        this.secret = secret.clone();
        this.lifetimeMillis = lifetimeMillis;
        this.clock = clock;
    }

    /**
     * Issues a signed bearer token for an authenticated user.
     *
     * @param user authenticated identity
     * @return bearer token
     */
    String issue(String user) {
        if (!validUser(user)) {
            throw new IllegalArgumentException("Invalid LDAP identity");
        }
        String payload = VERSION + "\n" + (clock.millis() + lifetimeMillis) + "\n" + user;
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return encoded + "." + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(sign(encoded.getBytes(StandardCharsets.US_ASCII)));
    }

    /**
     * Verifies a bearer token and returns its identity.
     *
     * @param token bearer token
     * @return authenticated identity
     * @throws IllegalArgumentException when the token is malformed, forged or expired
     */
    String verify(String token) {
        if (token == null || token.length() > MAX_TOKEN_LENGTH) {
            throw new IllegalArgumentException("Invalid LDAP session token");
        }
        int separator = token.indexOf('.');
        if (separator < 1 || separator != token.lastIndexOf('.')) {
            throw new IllegalArgumentException("Invalid LDAP session token");
        }
        try {
            String encoded = token.substring(0, separator);
            byte[] signature = Base64.getUrlDecoder().decode(token.substring(separator + 1));
            if (!MessageDigest.isEqual(signature, sign(encoded.getBytes(StandardCharsets.US_ASCII)))) {
                throw new IllegalArgumentException("Invalid LDAP session signature");
            }
            String[] parts = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8)
                    .split("\n", 3);
            if (parts.length != 3 || !VERSION.equals(parts[0]) || !validUser(parts[2])) {
                throw new IllegalArgumentException("Invalid LDAP session payload");
            }
            long expires = Long.parseLong(parts[1]);
            long now = clock.millis();
            if (expires <= now || expires > now + lifetimeMillis) {
                throw new IllegalArgumentException("Expired LDAP session token");
            }
            return parts[2];
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid LDAP session token", e);
        }
    }

    /**
     * Checks that an identity fits in a token payload.
     *
     * @param user identity
     * @return true when non-blank, short and on one line
     */
    private static boolean validUser(String user) {
        return user != null && !user.isBlank() && user.length() <= MAX_USER_LENGTH
                && user.indexOf('\n') < 0;
    }

    /**
     * Signs token bytes with HMAC SHA-256.
     *
     * @param payload signed bytes
     * @return signature
     */
    private byte[] sign(byte[] payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(payload);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC SHA-256 unavailable", e);
        }
    }
}
