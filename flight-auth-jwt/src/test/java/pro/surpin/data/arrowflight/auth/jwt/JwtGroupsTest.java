package pro.surpin.data.arrowflight.auth.jwt;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Groups of the last verified token per identity. */
@Tag("unit")
class JwtGroupsTest {
    private static final Clock CLOCK = Clock.fixed(Instant.ofEpochMilli(1_000_000), ZoneOffset.UTC);

    /** Without a groups claim every identity has no groups, seen or not. */
    @Test
    void disabledHasNoGroups() {
        JwtGroups groups = new JwtGroups(false, CLOCK);
        groups.put(new JwtVerifier.Verified("alice", Set.of("a"), 2_000_000));
        assertEquals(Set.of(), groups.groups("alice"));
        assertEquals(Set.of(), groups.groups("bob"));
    }

    /** The last token's groups win; an unseen or expired identity's groups are unknown (deny). */
    @Test
    void lastTokenUntilItExpires() {
        JwtGroups groups = new JwtGroups(true, CLOCK);
        groups.put(new JwtVerifier.Verified("alice", Set.of("a", "b"), 2_000_000));
        assertEquals(Set.of("a", "b"), groups.groups("alice"));
        groups.put(new JwtVerifier.Verified("alice", Set.of(), 2_000_000));
        assertEquals(Set.of(), groups.groups("alice"));
        assertThrows(IllegalStateException.class, () -> groups.groups("bob"));
        groups.put(new JwtVerifier.Verified("bob", Set.of("c"), 1_000_000));
        assertThrows(IllegalStateException.class, () -> groups.groups("bob"));
    }

    /** A full store drops expired entries first, then everything, never keeping stale groups. */
    @Test
    void boundedSize() {
        JwtGroups groups = new JwtGroups(true, CLOCK);
        for (int i = 0; i < 10_000; i++) {
            groups.put(new JwtVerifier.Verified("u" + i, Set.of("g"), i % 2 == 0 ? 1_000_000 : 2_000_000));
        }
        groups.put(new JwtVerifier.Verified("new", Set.of("n"), 2_000_000));
        assertEquals(Set.of("g"), groups.groups("u1"));
        assertEquals(Set.of("n"), groups.groups("new"));
        assertThrows(IllegalStateException.class, () -> groups.groups("u0"));
    }
}
