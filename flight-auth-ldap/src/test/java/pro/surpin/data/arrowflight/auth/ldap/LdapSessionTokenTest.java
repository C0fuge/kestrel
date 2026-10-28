package pro.surpin.data.arrowflight.auth.ldap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the signed LDAP session tokens. */
@Tag("unit")
class LdapSessionTokenTest {
    private static final Clock NOW = Clock.fixed(Instant.parse("2026-10-28T00:00:00Z"), ZoneOffset.UTC);

    /** A token verifies on every node with the same key, until it expires. */
    @Test
    void roundTripsUntilExpiry() {
        String token = new LdapSessionToken(new byte[32], 60_000, NOW).issue("alice");
        assertEquals("alice", new LdapSessionToken(new byte[32], 60_000, NOW).verify(token));
        assertEquals("alice", new LdapSessionToken(new byte[32], 60_000,
                Clock.offset(NOW, Duration.ofSeconds(59))).verify(token));
        assertThrows(IllegalArgumentException.class, () -> new LdapSessionToken(new byte[32], 60_000,
                Clock.offset(NOW, Duration.ofSeconds(60))).verify(token));
    }

    /** Another key, a changed payload or signature, and junk are refused. */
    @Test
    void refusesForgedTokens() {
        LdapSessionToken sessions = new LdapSessionToken(new byte[32], 60_000, NOW);
        String token = sessions.issue("alice");
        byte[] other = new byte[32];
        other[0] = 1;
        assertThrows(IllegalArgumentException.class,
                () -> new LdapSessionToken(other, 60_000, NOW).verify(token));
        String bob = sessions.issue("bob");
        String mixed = bob.substring(0, bob.indexOf('.')) + token.substring(token.indexOf('.'));
        assertThrows(IllegalArgumentException.class, () -> sessions.verify(mixed));
        for (String junk : new String[] {"", ".", "a.b.c", "abc", token + "x", null, "x".repeat(5000)}) {
            assertThrows(IllegalArgumentException.class, () -> sessions.verify(junk));
        }
        assertThrows(IllegalArgumentException.class, () -> sessions.issue("a\nb"));
        assertThrows(IllegalArgumentException.class, () -> new LdapSessionToken(new byte[31], 1, NOW));
    }

    /** A token claiming a lifetime longer than configured is refused. */
    @Test
    void refusesTooLongLifetimes() {
        String token = new LdapSessionToken(new byte[32], 3_600_000, NOW).issue("alice");
        assertThrows(IllegalArgumentException.class,
                () -> new LdapSessionToken(new byte[32], 60_000, NOW).verify(token));
    }
}
