package pro.surpin.data.arrowflight.server.services;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Signs and checks the caller identity of node-to-node calls (item 18, distributed complete
 * mode): the planning node reads the endpoints of a complete answer from its peers as the
 * client's user, in a header signed with the cluster's ticket secret. A peer that trusts the
 * header authorizes the call as that user, so with distributed complete mode on, the ticket
 * secret allows impersonating any user and must be protected like a keytab.
 */
public final class InternalCallIdentity {

    /** Call header that carries the signed identity. */
    public static final String HEADER = "x-kestrel-internal-identity";

    /** How long a signed identity is accepted after it was signed. */
    static final long VALIDITY_MILLIS = 60_000L;

    private static final String ALGORITHM = "HmacSHA256";
    private static final String CONTEXT = "kestrel-internal-identity-v1\n";
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    private final Supplier<byte[]> secret;

    /**
     * Creates a signer over the cluster's current ticket secret.
     *
     * @param secret returns the secret every node shares
     */
    public InternalCallIdentity(Supplier<byte[]> secret) {
        this.secret = Objects.requireNonNull(secret, "secret");
    }

    /**
     * Signs a user name for one node-to-node call.
     *
     * @param user the client's identity on the planning node; null is signed as empty
     * @param nowMillis current time
     * @return header value
     */
    public String sign(String user, long nowMillis) {
        String name = user == null ? "" : user;
        long expires = nowMillis + VALIDITY_MILLIS;
        return ENCODER.encodeToString(name.getBytes(StandardCharsets.UTF_8)) + "." + expires + "."
                + ENCODER.encodeToString(mac(name, expires));
    }

    /**
     * Checks a signed identity and returns its user name.
     *
     * @param header header value
     * @param nowMillis current time
     * @return the signed user name
     * @throws IllegalArgumentException when the value is malformed, expired, valid for too long
     *         or not signed with the cluster's secret
     */
    public String verify(String header, long nowMillis) {
        String[] parts = header == null ? new String[0] : header.trim().split("\\.", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException("Malformed internal call identity");
        }
        String name;
        long expires;
        byte[] presented;
        try {
            name = new String(DECODER.decode(parts[0]), StandardCharsets.UTF_8);
            expires = Long.parseLong(parts[1]);
            presented = DECODER.decode(parts[2]);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed internal call identity", e);
        }
        if (!MessageDigest.isEqual(presented, mac(name, expires))) {
            throw new IllegalArgumentException("Internal call identity is not signed by this cluster");
        }
        if (expires < nowMillis) {
            throw new IllegalArgumentException("Internal call identity has expired");
        }
        if (expires > nowMillis + 2 * VALIDITY_MILLIS) {
            // Signed far ahead: a clock that far off is not trusted either.
            throw new IllegalArgumentException("Internal call identity is valid for too long");
        }
        return name;
    }

    /**
     * Computes the signature of a user name and expiry time.
     *
     * @param name user name
     * @param expires expiry time in milliseconds
     * @return HMAC-SHA256 bytes
     */
    private byte[] mac(String name, long expires) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.get(), ALGORITHM));
            return mac.doFinal((CONTEXT + expires + "\n" + name).getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 is unavailable", e);
        }
    }
}
