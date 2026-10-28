package pro.surpin.data.arrowflight.server.services;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the signed caller identity of node-to-node calls. */
@Tag("unit")
class InternalCallIdentityTest {

    private static final long NOW = 1_800_000_000_000L;

    private final byte[] secret = filled(1);
    private final InternalCallIdentity identity = new InternalCallIdentity(() -> secret);

    /** Verifies a signed name comes back unchanged, including an empty and a non-ASCII name. */
    @Test
    void signedNameVerifies() {
        assertEquals("alice@EXAMPLE.COM", identity.verify(identity.sign("alice@EXAMPLE.COM", NOW), NOW));
        assertEquals("", identity.verify(identity.sign(null, NOW), NOW + 1_000));
        assertEquals("jürgen.a.b", identity.verify(identity.sign("jürgen.a.b", NOW), NOW));
    }

    /** Verifies another cluster's secret is refused. */
    @Test
    void otherSecretIsRefused() {
        String signed = new InternalCallIdentity(() -> filled(2)).sign("alice", NOW);
        assertThrows(IllegalArgumentException.class, () -> identity.verify(signed, NOW));
    }

    /** Verifies a changed name or expiry is refused. */
    @Test
    void tamperedValueIsRefused() {
        String[] parts = identity.sign("alice", NOW).split("\\.");
        String otherName = identity.sign("mallory", NOW).split("\\.")[0];
        assertThrows(IllegalArgumentException.class,
                () -> identity.verify(otherName + "." + parts[1] + "." + parts[2], NOW));
        long later = Long.parseLong(parts[1]) + 60_000;
        assertThrows(IllegalArgumentException.class,
                () -> identity.verify(parts[0] + "." + later + "." + parts[2], NOW));
    }

    /** Verifies an expired value and one valid too far ahead are refused. */
    @Test
    void timeLimitsAreEnforced() {
        String signed = identity.sign("alice", NOW);
        assertEquals("alice", identity.verify(signed, NOW + InternalCallIdentity.VALIDITY_MILLIS));
        assertThrows(IllegalArgumentException.class,
                () -> identity.verify(signed, NOW + InternalCallIdentity.VALIDITY_MILLIS + 1));
        String ahead = identity.sign("alice", NOW + 3 * InternalCallIdentity.VALIDITY_MILLIS);
        assertThrows(IllegalArgumentException.class, () -> identity.verify(ahead, NOW));
    }

    /** Verifies malformed values are refused. */
    @Test
    void malformedValuesAreRefused() {
        for (String value : new String[] {null, "", "a.b", "a.b.c.d", "!!.1.AA", "YQ.x.AA"}) {
            assertThrows(IllegalArgumentException.class, () -> identity.verify(value, NOW), value);
        }
    }

    private static byte[] filled(int value) {
        byte[] bytes = new byte[32];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }
}
