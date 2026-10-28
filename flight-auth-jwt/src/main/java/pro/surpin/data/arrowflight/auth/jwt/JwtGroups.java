package pro.surpin.data.arrowflight.auth.jwt;

import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Groups of the last token verified for each identity, kept until that token expires, so policy
 * checks (which see only the identity) use the groups of the call's own token.
 */
final class JwtGroups {
    private static final int MAX_ENTRIES = 10_000;
    private final boolean enabled;
    private final Clock clock;
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    /**
     * Groups of one token.
     *
     * @param groups group names
     * @param expiresAtMillis when the token stops being accepted
     */
    private record Entry(Set<String> groups, long expiresAtMillis) {
    }

    /**
     * Creates the store.
     *
     * @param enabled whether tokens carry groups; without, every identity has none
     * @param clock time source
     */
    JwtGroups(boolean enabled, Clock clock) {
        this.enabled = enabled;
        this.clock = clock;
    }

    /**
     * Remembers the groups of a verified token.
     *
     * @param token verified token
     */
    void put(JwtVerifier.Verified token) {
        if (!enabled) {
            return;
        }
        if (entries.size() >= MAX_ENTRIES && !entries.containsKey(token.identity())) {
            long now = clock.millis();
            entries.values().removeIf(entry -> entry.expiresAtMillis() <= now);
            if (entries.size() >= MAX_ENTRIES) {
                entries.clear();
            }
        }
        entries.put(token.identity(), new Entry(token.groups(), token.expiresAtMillis()));
    }

    /**
     * Returns the groups of an identity's last verified token.
     *
     * @param identity identity of a verified token
     * @return group names
     * @throws IllegalStateException when no unexpired token of the identity was seen, so the
     *     groups are unknown and policy checks deny
     */
    Set<String> groups(String identity) {
        if (!enabled) {
            return Set.of();
        }
        Entry entry = entries.get(identity);
        if (entry == null || entry.expiresAtMillis() <= clock.millis()) {
            throw new IllegalStateException("JWT groups unknown: no valid token of this caller seen");
        }
        return entry.groups();
    }
}
