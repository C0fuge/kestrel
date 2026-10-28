package pro.surpin.data.arrowflight.auth.ldap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Checks binds, searches, TLS and failover against an in-process directory. */
@Tag("unit")
class LdapDirectoryTest {
    @TempDir
    static Path dir;
    private static LdapTestDirectory ldap;
    private static LdapTestDirectory wrongName;
    private static LdapTestDirectory noStartTls;

    @BeforeAll
    static void start() throws Exception {
        ldap = new LdapTestDirectory(dir, "dns:localhost,ip:127.0.0.1", true);
        wrongName = new LdapTestDirectory(dir, "dns:other.example,ip:127.0.0.1", true);
        noStartTls = new LdapTestDirectory(dir, "dns:localhost", false);
    }

    @AfterAll
    static void stop() {
        ldap.close();
        wrongName.close();
        noStartTls.close();
    }

    /**
     * Builds a directory client.
     *
     * @param settings provider settings
     * @return client
     */
    private static LdapDirectory directory(Map<String, String> settings) {
        return new LdapDirectory(LdapSettings.fromEnvironment(settings));
    }

    /** A DN template bind over StartTLS accepts the right password only. */
    @Test
    void templateBindOverStartTls() throws Exception {
        LdapDirectory directory = directory(ldap.templateSettings());
        assertEquals("alice", directory.authenticate("alice", "alice-pw"));
        assertEquals("bob", directory.authenticate("bob", "bob:pw:with:colons"));
        assertNull(directory.authenticate("alice", "wrong"));
        assertNull(directory.authenticate("nobody", "alice-pw"));
    }

    /** Empty passwords (an unauthenticated bind would succeed) and unsafe names never reach the directory. */
    @Test
    void refusesEmptyPasswordsAndUnsafeNames() throws Exception {
        LdapDirectory directory = directory(ldap.searchSettings());
        assertNull(directory.authenticate("alice", ""));
        assertNull(directory.authenticate("alice", null));
        for (String name : new String[] {"*", "al*", "alice)(uid=*", "uid=alice,ou=people", "a b", "", "x\n"}) {
            assertNull(directory.authenticate(name, "alice-pw"), name);
        }
    }

    /** Search mode finds the entry with the service account and binds as it; the identity can be an attribute. */
    @Test
    void searchBindOverLdaps() throws Exception {
        Map<String, String> settings = ldap.searchSettings();
        settings.put("FLIGHT_LDAP_USER_SEARCH_BASE", "ou=people," + LdapTestDirectory.BASE);
        assertEquals("ALICE", directory(settings).authenticate("ALICE", "alice-pw"));
        settings.put("FLIGHT_LDAP_USER_ATTRIBUTE", "uid");
        LdapDirectory directory = directory(settings);
        assertEquals("alice", directory.authenticate("ALICE", "alice-pw"));
        assertNull(directory.authenticate("alice", "bob:pw:with:colons"));
        assertNull(directory.authenticate("nobody", "x"));
    }

    /** A name matching several entries is refused even with a right password. */
    @Test
    void refusesAmbiguousNames() throws Exception {
        LdapDirectory directory = directory(ldap.searchSettings());
        assertNull(directory.authenticate("carol", "carol-pw"));
        assertEquals("alice", directory.authenticate("alice", "alice-pw"));
    }

    /** A custom filter (Active Directory style) and an anonymous search work. */
    @Test
    void customFilterAndAnonymousSearch() throws Exception {
        Map<String, String> settings = ldap.searchSettings();
        settings.remove("FLIGHT_LDAP_BIND_DN");
        settings.remove("FLIGHT_LDAP_BIND_PASSWORD");
        settings.put("FLIGHT_LDAP_USER_SEARCH_FILTER", "(&(objectClass=inetOrgPerson)(sn={0}))");
        assertEquals("alice", directory(settings).authenticate("alice", "alice-pw"));
    }

    /** A refused service account is a configuration failure, not a wrong user password. */
    @Test
    void wrongServicePasswordIsUnavailable() {
        Map<String, String> settings = ldap.searchSettings();
        settings.put("FLIGHT_LDAP_BIND_PASSWORD", "wrong");
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(settings).authenticate("alice", "alice-pw"));
    }

    /** The next URL is tried when a server cannot be reached; none reachable is unavailable. */
    @Test
    void failsOverAndReportsUnreachable() throws Exception {
        int dead;
        try (ServerSocket socket = new ServerSocket(0)) {
            dead = socket.getLocalPort();
        }
        Map<String, String> settings = ldap.searchSettings();
        settings.put("FLIGHT_LDAP_URL", "ldaps://localhost:" + dead + " " + ldap.ldapsUrl("localhost"));
        assertEquals("alice", directory(settings).authenticate("alice", "alice-pw"));
        settings.put("FLIGHT_LDAP_URL", "ldaps://localhost:" + dead);
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(settings).authenticate("alice", "alice-pw"));
    }

    /** An untrusted certificate fails, over LDAPS and StartTLS. */
    @Test
    void refusesUntrustedCertificates() {
        Map<String, String> ldaps = ldap.searchSettings();
        ldaps.remove("FLIGHT_LDAP_TRUSTSTORE");
        ldaps.remove("FLIGHT_LDAP_TRUSTSTORE_PASSWORD");
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(ldaps).authenticate("alice", "alice-pw"));
        Map<String, String> startTls = ldap.templateSettings();
        startTls.put("FLIGHT_LDAP_TRUSTSTORE", wrongName.trustStore().toString());
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(startTls).authenticate("alice", "alice-pw"));
    }

    /** A trusted certificate for another host name fails, over LDAPS and StartTLS. */
    @Test
    void refusesCertificatesForOtherHosts() throws Exception {
        Map<String, String> ldaps = ldap.searchSettings();
        ldaps.put("FLIGHT_LDAP_TRUSTSTORE", wrongName.trustStore().toString());
        ldaps.put("FLIGHT_LDAP_URL", wrongName.ldapsUrl("127.0.0.1"));
        Map<String, String> byAddress = Map.copyOf(ldaps);
        assertEquals("alice", directory(byAddress).authenticate("alice", "alice-pw"));
        ldaps.put("FLIGHT_LDAP_URL", wrongName.ldapsUrl("localhost"));
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(ldaps).authenticate("alice", "alice-pw"));
        Map<String, String> startTls = ldap.templateSettings();
        startTls.put("FLIGHT_LDAP_TRUSTSTORE", wrongName.trustStore().toString());
        startTls.put("FLIGHT_LDAP_URL", wrongName.ldapUrl("127.0.0.1"));
        assertEquals("alice", directory(startTls).authenticate("alice", "alice-pw"));
        startTls.put("FLIGHT_LDAP_URL", wrongName.ldapUrl("localhost"));
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(startTls).authenticate("alice", "alice-pw"));
    }

    /** A server without StartTLS is unavailable, never used in clear text. */
    @Test
    void refusesServersWithoutStartTls() {
        Map<String, String> settings = ldap.templateSettings();
        settings.put("FLIGHT_LDAP_URL", noStartTls.ldapUrl("localhost"));
        settings.put("FLIGHT_LDAP_TRUSTSTORE", noStartTls.trustStore().toString());
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> directory(settings).authenticate("alice", "alice-pw"));
    }

    /** Plain LDAP is used only when explicitly allowed. */
    @Test
    void plaintextOnlyWhenAllowed() throws Exception {
        Map<String, String> settings = ldap.templateSettings();
        settings.remove("FLIGHT_LDAP_START_TLS");
        assertThrows(IllegalArgumentException.class, () -> LdapSettings.fromEnvironment(settings));
        settings.put("FLIGHT_LDAP_ALLOW_PLAINTEXT", "true");
        assertEquals("alice", directory(settings).authenticate("alice", "alice-pw"));
    }

    /**
     * Adds the group search to settings.
     *
     * @param settings provider settings
     * @return the same settings
     */
    private static Map<String, String> withGroups(Map<String, String> settings) {
        settings.put("FLIGHT_LDAP_GROUP_SEARCH_BASE", "ou=groups," + LdapTestDirectory.BASE);
        return settings;
    }

    /** Without a group search, logins carry no groups and identities have none. */
    @Test
    void noGroupsWithoutGroupSearch() throws Exception {
        LdapDirectory directory = directory(ldap.searchSettings());
        assertEquals(Set.of(), directory.login("alice", "alice-pw").groups());
        assertEquals(Set.of(), directory.groups("alice"));
    }

    /**
     * The default filter finds groupOfNames, groupOfUniqueNames and posixGroup memberships, at the login
     * (as the user, template mode) and for a session identity (anonymous search).
     */
    @Test
    void readsGroupsAtLoginAndForIdentities() throws Exception {
        LdapDirectory directory = directory(withGroups(ldap.templateSettings()));
        assertEquals(new LdapFlightAuthenticator.Login("alice", Set.of("analysts", "bi-users")),
                directory.login("alice", "alice-pw"));
        assertEquals(Set.of("bi-users", "ops"), directory.login("bob", "bob:pw:with:colons").groups());
        assertNull(directory.login("bob", "wrong"));
        assertEquals(Set.of("analysts", "bi-users"), directory.groups("alice"));
        assertEquals(Set.of("bi-users", "ops"), directory.groups("bob"));
        assertEquals(Set.of(), directory.groups("nobody"));
    }

    /**
     * In search mode with an identity attribute, the service account finds the entry of a session
     * identity; a custom filter and name attribute apply.
     */
    @Test
    void searchModeIdentityGroupsWithCustomFilter() throws Exception {
        Map<String, String> settings = withGroups(ldap.searchSettings());
        settings.put("FLIGHT_LDAP_USER_SEARCH_BASE", "ou=people," + LdapTestDirectory.BASE);
        settings.put("FLIGHT_LDAP_USER_ATTRIBUTE", "uid");
        LdapDirectory directory = directory(settings);
        assertEquals(new LdapFlightAuthenticator.Login("alice", Set.of("analysts", "bi-users")),
                directory.login("ALICE", "alice-pw"));
        assertEquals(Set.of("bi-users", "ops"), directory.groups("bob"));
        settings.put("FLIGHT_LDAP_GROUP_SEARCH_FILTER", "(&(objectClass=groupOfUniqueNames)(uniqueMember={1}))");
        settings.put("FLIGHT_LDAP_GROUP_NAME_ATTRIBUTE", "description");
        directory = directory(settings);
        assertEquals(Set.of("BI users"), directory.groups("bob"));
        assertEquals(Set.of("BI users"), directory.login("alice", "alice-pw").groups());
    }

    /** An identity without exactly one entry, or a failing group search, fails instead of giving no groups. */
    @Test
    void groupLookupFailuresAreReported() {
        Map<String, String> settings = withGroups(ldap.searchSettings());
        LdapDirectory directory = directory(settings);
        assertThrows(LdapDirectory.DirectoryUnavailableException.class, () -> directory.groups("carol"));
        assertThrows(LdapDirectory.DirectoryUnavailableException.class, () -> directory.groups("nobody"));
        settings.put("FLIGHT_LDAP_GROUP_SEARCH_BASE", "ou=missing," + LdapTestDirectory.BASE);
        LdapDirectory missingBase = directory(settings);
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> missingBase.login("alice", "alice-pw"));
        assertThrows(LdapDirectory.DirectoryUnavailableException.class, () -> missingBase.groups("alice"));
        Map<String, String> template = withGroups(ldap.templateSettings());
        template.put("FLIGHT_LDAP_BIND_DN", LdapTestDirectory.SERVICE_DN);
        template.put("FLIGHT_LDAP_BIND_PASSWORD", "wrong");
        LdapDirectory wrongService = directory(template);
        assertThrows(LdapDirectory.DirectoryUnavailableException.class, () -> wrongService.groups("alice"));
        assertThrows(LdapDirectory.DirectoryUnavailableException.class,
                () -> wrongService.login("alice", "alice-pw"));
    }
}
