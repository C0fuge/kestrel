package pro.surpin.data.arrowflight.auth.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Validation of the FLIGHT_JWT_* settings. */
@Tag("unit")
class JwtSettingsTest {

    /**
     * Minimal valid settings with a key set address.
     *
     * @return mutable settings
     */
    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put("FLIGHT_JWT_JWKS_URL", "https://idp.test/realms/data/protocol/openid-connect/certs");
        settings.put("FLIGHT_JWT_ISSUER", "https://idp.test/realms/data");
        settings.put("FLIGHT_JWT_AUDIENCE", "flight");
        return settings;
    }

    /**
     * Asserts that settings are refused with a message containing a text.
     *
     * @param settings settings
     * @param text expected part of the message
     */
    private static void refused(Map<String, String> settings, String text) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> JwtSettings.fromEnvironment(settings));
        assertTrue(e.getMessage().contains(text), e.getMessage());
    }

    /** Defaults: all asymmetric algorithms, sub, no groups, 60 s skew, 15 min cache, Basic off. */
    @Test
    void defaults() {
        JwtSettings settings = JwtSettings.fromEnvironment(minimal());
        assertEquals(JwtSettings.SUPPORTED, settings.algorithms());
        assertEquals("sub", settings.userClaim());
        assertNull(settings.groupsClaim());
        assertEquals(60, settings.clockSkewSeconds());
        assertEquals(15 * 60_000L, settings.jwksCacheMillis());
        assertEquals(30_000L, settings.jwksRefreshLimitMillis());
        assertEquals(Set.of("flight"), settings.audiences());
        assertFalse(settings.basicPassword());
        assertFalse(settings.algorithms().stream().anyMatch(a -> a.getName().startsWith("HS")));
    }

    /** Explicit values are taken. */
    @Test
    void explicitValues() {
        Map<String, String> values = minimal();
        values.put("FLIGHT_JWT_AUDIENCE", "flight, account");
        values.put("FLIGHT_JWT_ALGORITHMS", "rs256 ES384");
        values.put("FLIGHT_JWT_USER_CLAIM", "preferred_username");
        values.put("FLIGHT_JWT_GROUPS_CLAIM", "realm_access.roles");
        values.put("FLIGHT_JWT_CLOCK_SKEW_SECONDS", "0");
        values.put("FLIGHT_JWT_BASIC_PASSWORD", "true");
        JwtSettings settings = JwtSettings.fromEnvironment(values);
        assertEquals(Set.of("flight", "account"), settings.audiences());
        assertEquals(Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES384), settings.algorithms());
        assertEquals("preferred_username", settings.userClaim());
        assertEquals("realm_access.roles", settings.groupsClaim());
        assertEquals(0, settings.clockSkewSeconds());
        assertTrue(settings.basicPassword());
    }

    /** Missing, conflicting, insecure and malformed settings are refused. */
    /** The nimbus claims verifier asks {@code contains(null)}; an immutable {@code Set.of} copy would throw. */
    @Test
    void setsAnswerContainsNull() {
        Map<String, String> values = minimal();
        values.put("FLIGHT_JWT_AUDIENCE", "flight");
        JwtSettings settings = JwtSettings.fromEnvironment(values);
        assertFalse(settings.audiences().contains(null));
        assertFalse(settings.algorithms().contains(null));
    }

    @Test
    void refusesBadSettings() {
        Map<String, String> none = minimal();
        none.remove("FLIGHT_JWT_JWKS_URL");
        refused(none, "exactly one");
        Map<String, String> both = minimal();
        both.put("FLIGHT_JWT_JWKS_FILE", "/x.json");
        refused(both, "exactly one");
        for (String url : new String[] {"http://idp.test/certs", "file:/etc/jwks.json", "https:///x",
                "https://user@idp.test/x", "https://idp test"}) {
            Map<String, String> bad = minimal();
            bad.put("FLIGHT_JWT_JWKS_URL", url);
            refused(bad, "FLIGHT_JWT_JWKS_URL");
        }
        for (String name : new String[] {"FLIGHT_JWT_ISSUER", "FLIGHT_JWT_AUDIENCE"}) {
            Map<String, String> missing = minimal();
            missing.put(name, " ");
            refused(missing, name);
        }
        for (String algorithms : new String[] {"HS256", "none", "RS256,HS512", "EdDSA"}) {
            Map<String, String> bad = minimal();
            bad.put("FLIGHT_JWT_ALGORITHMS", algorithms);
            refused(bad, "not accepted");
        }
        Map<String, String> claim = minimal();
        claim.put("FLIGHT_JWT_USER_CLAIM", "a.b");
        refused(claim, "FLIGHT_JWT_USER_CLAIM");
        Map<String, String> groups = minimal();
        groups.put("FLIGHT_JWT_GROUPS_CLAIM", "groups..x");
        refused(groups, "FLIGHT_JWT_GROUPS_CLAIM");
        Map<String, String> skew = minimal();
        skew.put("FLIGHT_JWT_CLOCK_SKEW_SECONDS", "601");
        refused(skew, "between");
        Map<String, String> flag = minimal();
        flag.put("FLIGHT_JWT_BASIC_PASSWORD", "yes");
        refused(flag, "true or false");
        Map<String, String> store = minimal();
        store.remove("FLIGHT_JWT_JWKS_URL");
        store.put("FLIGHT_JWT_JWKS_FILE", "/x.json");
        store.put("FLIGHT_JWT_TRUSTSTORE", "/t.p12");
        refused(store, "FLIGHT_JWT_TRUSTSTORE");
    }
}
