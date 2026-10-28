package pro.surpin.data.arrowflight.auth.ldap;

import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Remembers the directory groups of logged-in identities for a session's lifetime, so policy checks
 * do not ask the directory on every call; a node that has not seen the login asks the directory once.
 */
final class LdapGroupCache {
    private static final int MAX_ENTRIES = 10_000;
    private final LdapFlightAuthenticator.Directory directory;
    private final boolean enabled;
    private final long ttlMillis;
    private final Clock clock;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    /**
     * Groups of one identity and when they must be read again.
     *
     * @param groups group names
     * @param expiresAtMillis expiry time
     */
    private record Entry(Set<String> groups, long expiresAtMillis) {
    }

    /**
     * Creates a cache.
     *
     * @param directory group lookup for identities not in the cache
     * @param enabled whether a group search is configured; without one every identity has no groups
     * @param ttlMillis how long groups are kept
     * @param clock time source
     */
    LdapGroupCache(LdapFlightAuthenticator.Directory directory, boolean enabled, long ttlMillis, Clock clock) {
        this.directory = directory;
        this.enabled = enabled;
        this.ttlMillis = ttlMillis;
        this.clock = clock;
    }

    /**
     * Stores the groups read at a login.
     *
     * @param identity peer identity
     * @param groups group names
     */
    void put(String identity, Set<String> groups) {
        if (!enabled) {
            return;
        }
        if (entries.size() >= MAX_ENTRIES) {
            long now = clock.millis();
            entries.values().removeIf(entry -> entry.expiresAtMillis() <= now);
            if (entries.size() >= MAX_ENTRIES) {
                entries.clear();
            }
        }
        entries.put(identity, new Entry(Set.copyOf(groups), clock.millis() + ttlMillis));
    }

    /**
     * Returns the groups of an identity, asking the directory when they are unknown or expired.
     *
     * @param identity peer identity
     * @return group names, empty without a group search
     * @throws LdapDirectory.DirectoryUnavailableException when the directory cannot tell the groups
     */
    Set<String> groups(String identity) throws LdapDirectory.DirectoryUnavailableException {
        if (!enabled) {
            return Set.of();
        }
        Entry entry = entries.get(identity);
        if (entry != null && entry.expiresAtMillis() > clock.millis()) {
            return entry.groups();
        }
        Set<String> groups = directory.groups(identity);
        put(identity, groups);
        return Set.copyOf(groups);
    }
}
