package pro.surpin.data.arrowflight.auth.ldap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests keeping, expiring and looking up identity groups. */
@Tag("unit")
class LdapGroupCacheTest {
    /** A movable clock. */
    private static final class TestClock extends Clock {
        private long millis = 1_000_000;

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }
    }

    /**
     * Builds a directory that counts lookups and answers with fixed groups.
     *
     * @param lookups lookup counter
     * @param groups groups returned
     * @return directory
     */
    private static LdapFlightAuthenticator.Directory directory(AtomicInteger lookups, Set<String> groups) {
        return new LdapFlightAuthenticator.Directory() {
            @Override
            public LdapFlightAuthenticator.Login login(String user, String password) {
                return null;
            }

            @Override
            public Set<String> groups(String identity) {
                lookups.incrementAndGet();
                return groups;
            }
        };
    }

    /** Groups from a login are served until they expire; then the directory is asked again. */
    @Test
    void servesLoginGroupsUntilExpiry() throws Exception {
        AtomicInteger lookups = new AtomicInteger();
        TestClock clock = new TestClock();
        LdapGroupCache cache = new LdapGroupCache(directory(lookups, Set.of("new")), true, 60_000, clock);
        cache.put("alice", Set.of("old"));
        assertEquals(Set.of("old"), cache.groups("alice"));
        clock.millis += 59_999;
        assertEquals(Set.of("old"), cache.groups("alice"));
        assertEquals(0, lookups.get());
        clock.millis += 1;
        assertEquals(Set.of("new"), cache.groups("alice"));
        assertEquals(Set.of("new"), cache.groups("alice"));
        assertEquals(1, lookups.get());
        assertEquals(Set.of("new"), cache.groups("bob"));
        assertEquals(2, lookups.get());
    }

    /** Without a group search nothing is stored or looked up. */
    @Test
    void disabledCacheHasNoGroups() throws Exception {
        AtomicInteger lookups = new AtomicInteger();
        LdapGroupCache cache = new LdapGroupCache(directory(lookups, Set.of("x")), false, 60_000,
                Clock.systemUTC());
        cache.put("alice", Set.of("old"));
        assertEquals(Set.of(), cache.groups("alice"));
        assertEquals(0, lookups.get());
    }

    /** A failing lookup is reported and nothing is remembered for the identity. */
    @Test
    void failuresAreNotRemembered() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        LdapFlightAuthenticator.Directory flaky = new LdapFlightAuthenticator.Directory() {
            @Override
            public LdapFlightAuthenticator.Login login(String user, String password) {
                return null;
            }

            @Override
            public Set<String> groups(String identity) throws LdapDirectory.DirectoryUnavailableException {
                if (calls.incrementAndGet() == 1) {
                    throw new LdapDirectory.DirectoryUnavailableException("down", null);
                }
                return Set.of("analysts");
            }
        };
        LdapGroupCache cache = new LdapGroupCache(flaky, true, 60_000, Clock.systemUTC());
        assertThrows(LdapDirectory.DirectoryUnavailableException.class, () -> cache.groups("alice"));
        assertEquals(Set.of("analysts"), cache.groups("alice"));
    }
}
