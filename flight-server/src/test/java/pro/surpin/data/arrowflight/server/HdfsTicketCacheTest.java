package pro.surpin.data.arrowflight.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;

import org.apache.kerby.kerberos.kerb.ccache.CredentialCache;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.server.plugin.ConfigSource;

/**
 * The HDFS ticket cache writer against an in-process KDC (Apache Kerby): the cache file it writes
 * from a keytab login is read by the JDK, by Kerby and, where installed, by MIT Kerberos (the
 * library libhdfs3 uses), and its ticket-granting ticket gets service tickets.
 */
@Tag("integration")
class HdfsTicketCacheTest {

    private static final String REALM = "EXAMPLE.COM";
    private static final String CLIENT = "flight/localhost@" + REALM;
    private static final String SERVICE = "hdfs/localhost@" + REALM;

    @TempDir
    static Path dir;
    static SimpleKdcServer kdc;
    static Path keytab;
    static Path krb5;
    static String previousKrb5Conf;

    @BeforeAll
    static void start() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(Files.createDirectories(dir.resolve("kdc")).toFile());
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        kdc.setKdcTcpPort(port);
        kdc.init();
        kdc.start();
        keytab = dir.resolve("flight.keytab");
        kdc.createAndExportPrincipals(keytab.toFile(), "flight/localhost");
        kdc.createPrincipal("hdfs/localhost", "hdfs-password");
        krb5 = dir.resolve("krb5.conf");
        Files.writeString(krb5, "[libdefaults]\n default_realm = " + REALM + "\n"
                + " udp_preference_limit = 1\n dns_lookup_kdc = false\n dns_lookup_realm = false\n"
                + " dns_canonicalize_hostname = false\n rdns = false\n"
                + "[realms]\n " + REALM + " = {\n  kdc = localhost:" + port + "\n }\n");
        previousKrb5Conf = System.setProperty("java.security.krb5.conf", krb5.toString());
    }

    @AfterAll
    static void stop() throws Exception {
        if (kdc != null) {
            kdc.stop();
        }
        if (previousKrb5Conf == null) {
            System.clearProperty("java.security.krb5.conf");
        } else {
            System.setProperty("java.security.krb5.conf", previousKrb5Conf);
        }
    }

    @Test
    void theCacheIsReadByJdkAndKerbyAndGetsServiceTickets() throws Exception {
        Path cache = Files.createDirectories(dir.resolve("jdk")).resolve("krb5cc_flight");
        new HdfsTicketCache(settings(cache)).refresh();
        assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(cache)));
        assertEquals(List.of(cache), listing(cache.getParent()), "no temporary file is left");

        CredentialCache kerby = new CredentialCache();
        kerby.load(cache.toFile());
        assertEquals(CLIENT, kerby.getPrimaryPrincipal().getName());
        assertEquals("krbtgt/" + REALM + "@" + REALM, kerby.getCredentials().get(0).getServerName().getName());

        Subject subject = cacheLogin(cache);
        KerberosTicket tgt = subject.getPrivateCredentials(KerberosTicket.class).iterator().next();
        assertEquals(CLIENT, tgt.getClient().getName());
        assertTrue(tgt.getEndTime().getTime() > System.currentTimeMillis());
        byte[] token = Subject.callAs(subject, () -> {
            GSSManager manager = GSSManager.getInstance();
            GSSContext context = manager.createContext(manager.createName(SERVICE, GSSName.NT_USER_NAME),
                    new Oid("1.2.840.113554.1.2.2"), null, GSSContext.DEFAULT_LIFETIME);
            try {
                return context.initSecContext(new byte[0], 0, 0);
            } finally {
                context.dispose();
            }
        });
        assertTrue(token.length > 0, "the cached ticket-granting ticket gets a service ticket");
    }

    @Test
    void mitKerberosReadsTheCacheAndGetsAServiceTicket() throws Exception {
        assumeTrue(onPath("klist") && onPath("kvno"), "MIT Kerberos tools (krb5-user) not installed");
        Path cache = Files.createDirectories(dir.resolve("mit")).resolve("krb5cc_flight");
        new HdfsTicketCache(settings(cache)).refresh();
        String klist = run("klist", "-f", "-c", "FILE:" + cache);
        assertTrue(klist.contains("Default principal: " + CLIENT), klist);
        assertTrue(klist.contains("krbtgt/" + REALM + "@" + REALM), klist);
        String kvno = run("kvno", "-c", "FILE:" + cache, SERVICE);
        assertTrue(kvno.contains(SERVICE + ": kvno = "), kvno);
        assertTrue(run("klist", "-c", "FILE:" + cache).contains(SERVICE), "the service ticket is cached");
    }

    @Test
    void aRefreshReplacesTheCacheWithANewTicket() throws Exception {
        Path cache = Files.createDirectories(dir.resolve("refresh")).resolve("krb5cc_flight");
        HdfsTicketCache writer = new HdfsTicketCache(settings(cache));
        long delay = writer.refresh();
        assertTrue(delay >= HdfsTicketCache.MIN_DELAY_MILLIS, "delay " + delay);
        assertTrue(delay < TimeUnit.DAYS.toMillis(1), "delay " + delay);
        byte[] first = Files.readAllBytes(cache);
        writer.refresh();
        byte[] second = Files.readAllBytes(cache);
        assertNotEquals(new String(first, StandardCharsets.ISO_8859_1),
                new String(second, StandardCharsets.ISO_8859_1), "a new session key");
        assertEquals(List.of(cache), listing(cache.getParent()));
        writer.close();
    }

    @Test
    void aFailedLoginWritesNothingAndNamesPrincipalAndKeytab() throws Exception {
        Path cache = Files.createDirectories(dir.resolve("failed")).resolve("krb5cc_flight");
        HdfsTicketCache writer = new HdfsTicketCache(new HdfsTicketCache.Settings("nobody@" + REALM,
                keytab.toString(), cache, cache));
        IOException e = assertThrows(IOException.class, writer::refresh);
        assertTrue(e.getMessage().contains("nobody@" + REALM) && e.getMessage().contains(keytab.toString()),
                e.getMessage());
        assertFalse(Files.exists(cache));
        Map<String, String> env = Map.of("HDFS_KERBEROS_PRINCIPAL", "nobody@" + REALM,
                "HDFS_KERBEROS_KEYTAB", keytab.toString(), "HDFS_KERBEROS_TICKET_CACHE", cache.toString());
        assertThrows(IllegalStateException.class, () -> HdfsTicketCache.start(source(env), Map.of()));
        assertFalse(Files.exists(cache));
    }

    @Test
    void startWritesTheCacheTheExtensionReads() throws Exception {
        Path cache = Files.createDirectories(dir.resolve("start")).resolve("krb5cc_libhdfs3");
        Map<String, String> sys = Map.of("hdfsKerberosPrincipal", CLIENT,
                "hdfsKerberosKeytab", keytab.toString());
        ConfigSource source = new ConfigSource(sys::get, new Properties(), Map.of(), null);
        Optional<HdfsTicketCache> started = HdfsTicketCache.start(source,
                Map.of(HdfsTicketCache.LIBHDFS3_TICKET_CACHE, cache.toString()));
        assertTrue(started.isPresent());
        started.get().close();
        assertEquals(CLIENT, cacheLogin(cache).getPrincipals().iterator().next().getName());
    }

    @Test
    void settingsAreOffUnlessPrincipalAndKeytabAreSet() {
        assertEquals(Optional.empty(), HdfsTicketCache.settings(source(Map.of()), Map.of(), "/tmp/x", 1000));
        assertThrows(IllegalArgumentException.class, () -> HdfsTicketCache.settings(
                source(Map.of("HDFS_KERBEROS_PRINCIPAL", CLIENT)), Map.of(), null, 1000));
        assertThrows(IllegalArgumentException.class, () -> HdfsTicketCache.settings(
                source(Map.of("HDFS_KERBEROS_KEYTAB", "/k")), Map.of(), null, 1000));
        assertThrows(IllegalArgumentException.class, () -> HdfsTicketCache.settings(
                source(Map.of("HDFS_KERBEROS_TICKET_CACHE", "/c")), Map.of(), null, 1000));
    }

    @Test
    void theCacheDefaultsToTheOneTheExtensionReads() {
        Map<String, String> login = new HashMap<>(Map.of("HDFS_KERBEROS_PRINCIPAL", CLIENT,
                "HDFS_KERBEROS_KEYTAB", "/k"));
        Map<String, String> conf = Map.of(HdfsTicketCache.LIBHDFS3_TICKET_CACHE, "/run/flight/cc");
        assertEquals(Path.of("/run/flight/cc"),
                HdfsTicketCache.settings(source(login), conf, "FILE:/tmp/env", 1000).orElseThrow().cache());
        assertEquals(Path.of("/tmp/env"),
                HdfsTicketCache.settings(source(login), Map.of(), "FILE:/tmp/env", 1000).orElseThrow().cache());
        assertEquals(Path.of("/tmp/krb5cc_1000"),
                HdfsTicketCache.settings(source(login), Map.of(), null, 1000).orElseThrow().cache());
        assertThrows(IllegalArgumentException.class,
                () -> HdfsTicketCache.settings(source(login), Map.of(), null, -1));
        assertThrows(IllegalArgumentException.class,
                () -> HdfsTicketCache.settings(source(login), Map.of(), "KEYRING:persistent:1000", 1000));
        login.put("HDFS_KERBEROS_TICKET_CACHE", "FILE:/var/cc");
        HdfsTicketCache.Settings explicit = HdfsTicketCache.settings(source(login), Map.of(), null, 1000)
                .orElseThrow();
        assertEquals(Path.of("/var/cc"), explicit.cache());
        assertEquals(Path.of("/tmp/krb5cc_1000"), explicit.extensionCache());
        assertNull(HdfsTicketCache.settings(source(login), Map.of(), null, -1).orElseThrow().extensionCache());
        login.put("HDFS_KERBEROS_TICKET_CACHE", "DIR:/var/cc");
        assertThrows(IllegalArgumentException.class,
                () -> HdfsTicketCache.settings(source(login), Map.of(), null, 1000));
    }

    @Test
    void theNextLoginComesBeforeTheTicketEnds() {
        long hour = TimeUnit.HOURS.toMillis(1);
        assertEquals((long) (10 * hour * HdfsTicketCache.RENEW_AT), HdfsTicketCache.nextDelay(0, 10 * hour, 0));
        assertEquals(2 * 60_000 - HdfsTicketCache.MIN_REMAINING_MILLIS - 1_000,
                HdfsTicketCache.nextDelay(0, 2 * 60_000, 1_000), "a short ticket: a minute before its end");
        assertEquals(HdfsTicketCache.MIN_DELAY_MILLIS, HdfsTicketCache.nextDelay(0, hour, hour));
    }

    @Test
    void principalNamesAndFlagsAreEncodedAsMitDoes() {
        assertEquals(List.of("flight", "host"), HdfsTicketCache.components("flight/host@R", "R"));
        assertEquals(List.of("a/b", "c@d"), HdfsTicketCache.components("a\\/b/c\\@d@R", "R"));
        boolean[] flags = new boolean[32];
        flags[1] = true;
        flags[8] = true;
        assertEquals(0x40800000, HdfsTicketCache.flags(flags));
        assertEquals(0, HdfsTicketCache.flags(null));
    }

    private static HdfsTicketCache.Settings settings(Path cache) {
        return new HdfsTicketCache.Settings(CLIENT, keytab.toString(), cache, cache);
    }

    private static ConfigSource source(Map<String, String> env) {
        return new ConfigSource(name -> null, new Properties(), env, null);
    }

    private static List<Path> listing(Path directory) throws IOException {
        try (var files = Files.list(directory)) {
            return files.toList();
        }
    }

    private static Subject cacheLogin(Path cache) throws Exception {
        Map<String, String> options = Map.of("useTicketCache", "true", "ticketCache", cache.toString(),
                "doNotPrompt", "true", "refreshKrb5Config", "true");
        AppConfigurationEntry entry = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        Subject subject = new Subject();
        new LoginContext("cache", subject, null, new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {entry};
            }
        }).login();
        return subject;
    }

    private static boolean onPath(String tool) {
        for (String part : System.getenv().getOrDefault("PATH", "").split(":")) {
            if (!part.isEmpty() && Files.isExecutable(Path.of(part, tool))) {
                return true;
            }
        }
        return false;
    }

    private static String run(String... command) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("KRB5_CONFIG", krb5.toString());
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(process.waitFor(30, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue(), String.join(" ", command) + ": " + output);
        return output;
    }
}
