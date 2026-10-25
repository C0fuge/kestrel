package pro.surpin.data.arrowflight.plugin.hive;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.kerby.kerberos.kerb.client.KrbClient;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.kerby.kerberos.kerb.type.ticket.TgtTicket;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import pro.surpin.data.arrowflight.spi.catalog.Catalog;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * The hive catalog against a Kerberos metastore: an in-process KDC (Apache Kerby), a SASL
 * (GSSAPI) Thrift front as {@code hive/localhost} that hands every call to the real test
 * metastore, and the client logged in from a keytab or a ticket cache, with each protection
 * level. Wrong principals, missing credentials and protection levels the metastore does not
 * offer are errors.
 */
class KerberosMetastoreTest {

    private static final String REALM = "EXAMPLE.COM";

    @TempDir
    static Path dir;
    static SimpleKdcServer kdc;
    static IsolatedHms hms;
    static String root;
    static Path keytab;
    static Path ticketCache;
    static Subject hive;
    static String previousKrb5Conf;

    @BeforeAll
    static void start() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        Path work = Files.createDirectories(dir.resolve("kdc"));
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(work.toFile());
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        kdc.setKdcTcpPort(port);
        kdc.init();
        kdc.start();
        keytab = dir.resolve("flight.keytab");
        kdc.createAndExportPrincipals(keytab.toFile(), "flight/localhost");
        Path hiveKeytab = dir.resolve("hive.keytab");
        kdc.createAndExportPrincipals(hiveKeytab.toFile(), "hive/localhost");
        kdc.createPrincipal("cached", "cached-password");
        Path krb5 = dir.resolve("krb5.conf");
        Files.writeString(krb5, "[libdefaults]\n default_realm = " + REALM + "\n"
                + " udp_preference_limit = 1\n dns_lookup_kdc = false\n dns_lookup_realm = false\n"
                + " dns_canonicalize_hostname = false\n rdns = false\n"
                + "[realms]\n " + REALM + " = {\n  kdc = localhost:" + port + "\n }\n");
        previousKrb5Conf = System.setProperty("java.security.krb5.conf", krb5.toString());
        KrbClient client = kdc.getKrbClient();
        TgtTicket tgt = client.requestTgt("cached@" + REALM, "cached-password");
        ticketCache = dir.resolve("krb5cc_test");
        client.storeTicket(tgt, ticketCache.toFile());
        hive = acceptor("hive/localhost@" + REALM, hiveKeytab);

        hms = IsolatedHms.shared();
        hms.createDatabase("krb");
        Path lake = Files.createDirectories(dir.resolve("lake/t"));
        root = dir.resolve("lake").toString();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            s.execute("COPY (SELECT 1 AS id, 'a' AS name) TO '" + lake.resolve("part-0.parquet")
                    + "' (FORMAT parquet)");
        }
        hms.createTable("krb", "t", "file:" + lake, "id int;name string", "", "parquet", null);
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
    void aKeytabLoginReadsTheMetastoreWithTheStrongestProtection() throws Exception {
        try (Front front = new Front("auth-conf");
                Catalog catalog = open(front, Map.of("clientPrincipal", "flight/localhost@" + REALM,
                        "clientKeytab", keytab.toString()));
                Catalog plain = open(null, Map.of())) {
            assertTrue(catalog.schemas().contains("krb"), catalog.schemas().toString());
            assertEquals(List.of("t"), catalog.tables("krb"));
            TableName name = new TableName("krb", "t");
            TableSnapshot snapshot = catalog.current(name).orElseThrow();
            TableSnapshot expected = plain.current(name).orElseThrow();
            assertEquals(expected.columns(), snapshot.columns());
            assertEquals(expected.files(), snapshot.files());
            assertEquals(Set.of("flight/localhost@" + REALM), front.clients.keySet());
            assertEquals(Set.of("auth-conf"), Set.copyOf(front.clients.values()));
        }
    }

    @Test
    void aTicketCacheLoginReadsTheMetastore() throws Exception {
        for (String qop : List.of("auth", "auth-int")) {
            try (Front front = new Front(qop);
                    Catalog catalog = open(front, Map.of("clientTicketCache", ticketCache.toString(),
                            "saslQop", qop))) {
                assertEquals(List.of("t"), catalog.tables("krb"));
                assertEquals(Set.of("cached@" + REALM), front.clients.keySet());
                assertEquals(Set.of(qop), Set.copyOf(front.clients.values()));
            }
        }
    }

    @Test
    void handshakeAndLoginFailuresAreErrors() throws Exception {
        try (Front front = new Front("auth-conf")) {
            Map<String, String> login = Map.of("clientPrincipal", "flight/localhost@" + REALM,
                    "clientKeytab", keytab.toString());
            IOException unknown = assertThrows(IOException.class, () -> open(front, login,
                    "metastorePrincipal", "hive/elsewhere@" + REALM));
            assertTrue(unknown.getMessage().contains("Kerberos (SASL) handshake with hive/elsewhere"),
                    unknown.getMessage());
            Map<String, String> weak = new HashMap<>(login);
            weak.put("saslQop", "auth");
            IOException protection = assertThrows(IOException.class, () -> open(front, weak));
            assertTrue(protection.getMessage().contains("Kerberos (SASL) handshake"),
                    protection.getMessage());
            IOException noCache = assertThrows(IOException.class, () -> open(front,
                    Map.of("clientTicketCache", dir.resolve("missing").toString())));
            assertTrue(noCache.getMessage().contains("Kerberos login for the Hive Metastore failed"),
                    noCache.getMessage());
            IOException badKeytab = assertThrows(IOException.class, () -> open(front,
                    Map.of("clientPrincipal", "nobody@" + REALM, "clientKeytab", keytab.toString())));
            assertTrue(badKeytab.getMessage().contains("Kerberos login for the Hive Metastore failed"),
                    badKeytab.getMessage());
            assertTrue(front.clients.isEmpty(), front.clients.toString());
        }
        IOException plainClient = assertThrows(IOException.class, () -> {
            try (Front front = new Front("auth")) {
                open(null, Map.of("uris", "thrift://localhost:" + front.port, "connectTimeoutMillis",
                        "2000", "socketTimeoutMillis", "2000"));
            }
        });
        assertTrue(plainClient.getMessage().contains("Hive Metastore"), plainClient.getMessage());
    }

    @Test
    void settingsAreChecked() {
        Map<String, String> base = Map.of("authentication", "kerberos", "metastorePrincipal",
                "hive/_HOST@" + REALM);
        HiveSettings.Kerberos kerberos = settings(base).kerberos();
        assertEquals("hive/_HOST@" + REALM, kerberos.metastorePrincipal());
        assertNull(kerberos.clientPrincipal());
        assertEquals(HiveSettings.DEFAULT_QOP, kerberos.qop());
        assertNull(settings(Map.of()).kerberos());
        assertEquals("auth-int,auth", settings(with(base, "saslQop", " AUTH-INT , auth")).kerberos()
                .qop());
        assertThrows(IllegalArgumentException.class,
                () -> settings(Map.of("authentication", "kerberos")));
        assertThrows(IllegalArgumentException.class, () -> settings(with(base, "metastorePrincipal",
                "hive@" + REALM)));
        assertThrows(IllegalArgumentException.class, () -> settings(with(base, "clientKeytab", "/k")));
        assertThrows(IllegalArgumentException.class, () -> settings(with(with(with(base,
                "clientPrincipal", "a@B"), "clientKeytab", "/k"), "clientTicketCache", "/c")));
        assertThrows(IllegalArgumentException.class, () -> settings(with(base, "framedTransport",
                "true")));
        assertThrows(IllegalArgumentException.class, () -> settings(with(base, "saslQop", "privacy")));
        assertThrows(IllegalArgumentException.class, () -> settings(with(base, "saslQop", " , ")));
        assertThrows(IllegalArgumentException.class,
                () -> settings(Map.of("authentication", "ldap")));
    }

    @Test
    void theHostPlaceholderAndRenewalFollowHive() {
        assertArrayEquals(new String[] {"hive", "hms-1.example.com"},
                KerberosLogin.servicePrincipal("hive/_HOST@" + REALM, "HMS-1.Example.COM"));
        assertArrayEquals(new String[] {"hive", "fixed.host"},
                KerberosLogin.servicePrincipal("hive/fixed.host@" + REALM, "other"));
        long hour = 3_600_000;
        assertFalse(KerberosLogin.needsRenewal(0, 10 * hour, 7 * hour));
        assertTrue(KerberosLogin.needsRenewal(0, 10 * hour, 8 * hour));
        assertFalse(KerberosLogin.needsRenewal(0, 1_000_000, 10_000));
        assertTrue(KerberosLogin.needsRenewal(0, 100_000, 50_000), "less than a minute left");
        assertTrue(KerberosLogin.needsRenewal(0, hour, 2 * hour), "expired");
    }

    /**
     * A SASL (GSSAPI) metastore as {@code hive/localhost} in front of the test metastore.
     */
    static final class Front implements AutoCloseable {
        final Map<String, String> clients = new ConcurrentHashMap<>();
        final int port;
        private final TServer server;
        private final TSocket backend;

        Front(String qop) throws Exception {
            URI target = URI.create(hms.uri());
            backend = new TSocket(target.getHost(), target.getPort());
            backend.open();
            ThriftHiveMetastore.Client forward =
                    new ThriftHiveMetastore.Client(new TBinaryProtocol(backend));
            InvocationHandler handler = (proxy, method, args) -> {
                synchronized (forward) {
                    try {
                        return method.invoke(forward, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
            };
            ThriftHiveMetastore.Iface iface = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
                    ThriftHiveMetastore.Iface.class.getClassLoader(),
                    new Class<?>[] {ThriftHiveMetastore.Iface.class}, handler);
            TSaslServerTransport.Factory sasl = new TSaslServerTransport.Factory();
            sasl.addServerDefinition("GSSAPI", "hive", "localhost",
                    Map.of(Sasl.QOP, qop, Sasl.SERVER_AUTH, "true"), callbacks -> {
                        for (Callback callback : callbacks) {
                            if (callback instanceof AuthorizeCallback authorize) {
                                authorize.setAuthorized(authorize.getAuthenticationID()
                                        .equals(authorize.getAuthorizationID()));
                            }
                        }
                    });
            TTransportFactory factory = new TTransportFactory() {
                @Override
                public TTransport getTransport(TTransport base) throws TTransportException {
                    try {
                        TTransport transport = Subject.callAs(hive, () -> sasl.getTransport(base));
                        var server = ((TSaslServerTransport) transport).getSaslServer();
                        clients.put(server.getAuthorizationID(),
                                String.valueOf(server.getNegotiatedProperty(Sasl.QOP)));
                        return transport;
                    } catch (CompletionException e) {
                        throw new TTransportException(e.getCause());
                    }
                }
            };
            TServerSocket socket = new TServerSocket(0);
            port = socket.getServerSocket().getLocalPort();
            server = new TThreadPoolServer(new TThreadPoolServer.Args(socket)
                    .processor(new ThriftHiveMetastore.Processor<>(iface))
                    .transportFactory(factory).protocolFactory(new TBinaryProtocol.Factory())
                    .minWorkerThreads(1).maxWorkerThreads(8));
            Thread thread = new Thread(server::serve, "sasl-hms-" + port);
            thread.setDaemon(true);
            thread.start();
            for (int i = 0; i < 100 && !server.isServing(); i++) {
                Thread.sleep(20);
            }
        }

        @Override
        public void close() {
            server.stop();
            backend.close();
        }
    }

    private static Subject acceptor(String principal, Path keytabFile) throws Exception {
        Map<String, String> options = Map.of("useKeyTab", "true", "keyTab", keytabFile.toString(),
                "principal", principal, "storeKey", "true", "isInitiator", "false", "doNotPrompt",
                "true", "refreshKrb5Config", "true");
        AppConfigurationEntry entry = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        Subject subject = new Subject();
        new LoginContext("hms", subject, null, new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {entry};
            }
        }).login();
        return subject;
    }

    private static Map<String, String> with(Map<String, String> base, String key, String value) {
        Map<String, String> values = new HashMap<>(base);
        values.put(key, value);
        return values;
    }

    private static HiveSettings settings(Map<String, String> overrides) {
        Map<String, String> values = new HashMap<>();
        values.put("uris", "thrift://hms:9083");
        values.put("roots", "/lake");
        values.putAll(overrides);
        return HiveSettings.from(new TestSupport.MapConfig("hive", values));
    }

    private static Catalog open(Front front, Map<String, String> overrides, String... more)
            throws Exception {
        Map<String, String> values = new HashMap<>();
        values.put("roots", root);
        if (front == null) {
            values.put("uris", hms.uri());
        } else {
            values.put("uris", "thrift://localhost:" + front.port);
            values.put("authentication", "kerberos");
            values.put("metastorePrincipal", "hive/_HOST@" + REALM);
            values.put("connectTimeoutMillis", "5000");
            values.put("socketTimeoutMillis", "10000");
        }
        values.putAll(overrides);
        for (int i = 0; i + 1 < more.length; i += 2) {
            values.put(more[i], more[i + 1]);
        }
        TestSupport.LocalStores stores = new TestSupport.LocalStores();
        Catalog catalog = new HiveCatalogProvider().open(new TestSupport.Context(
                new TestSupport.MapConfig("hive", values), stores, new SecretRedactor(), null));
        stores.freeze();
        return catalog;
    }
}
