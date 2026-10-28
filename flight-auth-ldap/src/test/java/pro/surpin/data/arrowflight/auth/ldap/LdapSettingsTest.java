package pro.surpin.data.arrowflight.auth.ldap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests reading and validating FLIGHT_LDAP_* settings. */
@Tag("unit")
class LdapSettingsTest {
    /**
     * Minimal valid template-mode settings.
     *
     * @return mutable settings
     */
    private static Map<String, String> base() {
        Map<String, String> settings = new HashMap<>();
        settings.put("FLIGHT_LDAP_URL", "ldaps://dc1.example.org ldaps://dc2.example.org:3269");
        settings.put("FLIGHT_LDAP_USER_DN_TEMPLATE", "{0}@example.org");
        settings.put("FLIGHT_LDAP_SESSION_SECRET", LdapTestDirectory.SESSION_SECRET);
        return settings;
    }

    /**
     * Asserts that settings are refused with a message naming a setting.
     *
     * @param settings settings
     * @param expected text the message must contain
     */
    private static void refused(Map<String, String> settings, String expected) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> LdapSettings.fromEnvironment(settings));
        assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    /** URLs keep their order and get default ports; defaults apply. */
    @Test
    void readsDefaults() {
        LdapSettings settings = LdapSettings.fromEnvironment(base());
        assertEquals(List.of(new LdapSettings.Server("dc1.example.org", 636, true),
                new LdapSettings.Server("dc2.example.org", 3269, true)), settings.servers());
        assertEquals(10 * 60_000L, settings.sessionMillis());
        assertEquals(5_000, settings.connectTimeoutMillis());
        assertEquals("PKCS12", settings.trustStoreType());
        assertNull(settings.searchBase());
        Map<String, String> plain = base();
        plain.put("FLIGHT_LDAP_URL", "ldap://dc1.example.org");
        plain.put("FLIGHT_LDAP_START_TLS", "TRUE");
        assertEquals(389, LdapSettings.fromEnvironment(plain).servers().get(0).port());
        assertTrue(!LdapSettings.fromEnvironment(plain).toString().contains("SECRET"));
    }

    /** Clear-text LDAP, StartTLS over LDAPS and malformed URLs are refused. */
    @Test
    void refusesInsecureOrMalformedServers() {
        Map<String, String> settings = base();
        settings.put("FLIGHT_LDAP_URL", "ldap://dc1.example.org");
        refused(settings, "clear text");
        settings.put("FLIGHT_LDAP_URL", "ldaps://dc1.example.org");
        settings.put("FLIGHT_LDAP_START_TLS", "true");
        refused(settings, "FLIGHT_LDAP_START_TLS");
        settings.put("FLIGHT_LDAP_START_TLS", "yes");
        refused(settings, "true or false");
        settings.remove("FLIGHT_LDAP_START_TLS");
        for (String url : new String[] {"http://x", "ldaps://x/dc=org", "ldaps://u@x", "ldaps://", "dc1"}) {
            settings.put("FLIGHT_LDAP_URL", url);
            refused(settings, "LDAP URL");
        }
        settings.remove("FLIGHT_LDAP_URL");
        refused(settings, "FLIGHT_LDAP_URL");
    }

    /** Exactly one user mode; search-only settings are refused in template mode. */
    @Test
    void refusesMixedModes() {
        Map<String, String> settings = base();
        settings.put("FLIGHT_LDAP_USER_SEARCH_BASE", "dc=example,dc=org");
        refused(settings, "exactly one");
        settings.remove("FLIGHT_LDAP_USER_DN_TEMPLATE");
        settings.put("FLIGHT_LDAP_USER_SEARCH_FILTER", "(uid=alice)");
        refused(settings, "{0}");
        settings.put("FLIGHT_LDAP_USER_SEARCH_FILTER", "(sAMAccountName={0})");
        settings.put("FLIGHT_LDAP_BIND_DN", "cn=svc");
        refused(settings, "go together");
        settings.put("FLIGHT_LDAP_BIND_PASSWORD", "pw");
        assertEquals("pw", LdapSettings.fromEnvironment(settings).bindPassword());
        Map<String, String> template = base();
        template.put("FLIGHT_LDAP_USER_DN_TEMPLATE", "uid=alice");
        refused(template, "{0}");
        template.put("FLIGHT_LDAP_USER_DN_TEMPLATE", "uid={0}");
        template.put("FLIGHT_LDAP_USER_ATTRIBUTE", "uid");
        refused(template, "search mode");
    }

    /** The session key must be base64 of at least 32 bytes; bounds are checked. */
    @Test
    void checksSessionAndBounds() {
        Map<String, String> settings = base();
        settings.put("FLIGHT_LDAP_SESSION_SECRET", "c2hvcnQ=");
        refused(settings, "32 bytes");
        settings.put("FLIGHT_LDAP_SESSION_SECRET", "not base64!");
        refused(settings, "base64");
        settings.remove("FLIGHT_LDAP_SESSION_SECRET");
        refused(settings, "FLIGHT_LDAP_SESSION_SECRET");
        settings.put("FLIGHT_LDAP_SESSION_SECRET", LdapTestDirectory.SESSION_SECRET);
        settings.put("FLIGHT_LDAP_SESSION_MINUTES", "0");
        refused(settings, "FLIGHT_LDAP_SESSION_MINUTES");
        settings.put("FLIGHT_LDAP_SESSION_MINUTES", "abc");
        refused(settings, "FLIGHT_LDAP_SESSION_MINUTES");
        settings.put("FLIGHT_LDAP_SESSION_MINUTES", "60");
        assertEquals(3_600_000L, LdapSettings.fromEnvironment(settings).sessionMillis());
    }

    /** Passwords can come from files, but not from both a file and a value. */
    @Test
    void readsSecretFiles(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("bind-password");
        Files.writeString(file, "from-file\n");
        Map<String, String> settings = base();
        settings.remove("FLIGHT_LDAP_USER_DN_TEMPLATE");
        settings.put("FLIGHT_LDAP_USER_SEARCH_BASE", "dc=example,dc=org");
        settings.put("FLIGHT_LDAP_BIND_DN", "cn=svc");
        settings.put("FLIGHT_LDAP_BIND_PASSWORD_FILE", file.toString());
        assertEquals("from-file", LdapSettings.fromEnvironment(settings).bindPassword());
        settings.put("FLIGHT_LDAP_BIND_PASSWORD", "pw");
        refused(settings, "not both");
    }
}
