package pro.surpin.data.arrowflight.auth.ldap;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.schema.Schema;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;
import com.unboundid.util.ssl.TrustAllTrustManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** An in-process directory with a StartTLS-capable LDAP listener and an LDAPS listener, for tests. */
final class LdapTestDirectory implements AutoCloseable {
    static final String BASE = "dc=example,dc=org";
    static final String SERVICE_DN = "cn=svc,dc=example,dc=org";
    static final String SERVICE_PASSWORD = "svc-secret";
    static final String PASSWORD = "store-pass";
    static final String SESSION_SECRET = Base64.getEncoder().encodeToString(new byte[32]);
    private final InMemoryDirectoryServer server;
    private final Path trustStore;
    private final Path keyStore;

    /**
     * Starts a directory whose certificate names the given host.
     *
     * @param dir scratch directory for key material
     * @param san subject alternative names of the server certificate, keytool syntax
     * @param startTls whether the plain listener offers StartTLS
     * @throws Exception when the directory cannot start
     */
    LdapTestDirectory(Path dir, String san, boolean startTls) throws Exception {
        keyStore = dir.resolve("server-" + Math.abs(san.hashCode()) + ".p12");
        trustStore = dir.resolve("trust-" + Math.abs(san.hashCode()) + ".p12");
        Path cert = dir.resolve("server-" + Math.abs(san.hashCode()) + ".crt");
        keytool("-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=ldap-test", "-ext", "SAN=" + san, "-validity", "2",
                "-keystore", keyStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
        keytool("-exportcert", "-alias", "server", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", cert.toString());
        keytool("-importcert", "-noprompt", "-alias", "ca", "-file", cert.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
        SSLUtil serverSsl = new SSLUtil(new KeyStoreKeyManager(keyStore.toFile(), PASSWORD.toCharArray(),
                "PKCS12", "server"), new TrustAllTrustManager());
        SSLUtil clientSsl = new SSLUtil(new TrustAllTrustManager());
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig(BASE);
        config.addAdditionalBindCredentials(SERVICE_DN, SERVICE_PASSWORD);
        config.setSchema(Schema.mergeSchemas(Schema.getDefaultStandardSchema(), new Schema(new Entry(
                "cn=schema",
                new com.unboundid.ldap.sdk.Attribute("attributeTypes",
                        "( 1.3.6.1.1.1.1.1 NAME 'gidNumber' EQUALITY integerMatch "
                                + "SYNTAX 1.3.6.1.4.1.1466.115.121.1.27 SINGLE-VALUE )",
                        "( 1.3.6.1.1.1.1.12 NAME 'memberUid' EQUALITY caseExactIA5Match "
                                + "SYNTAX 1.3.6.1.4.1.1466.115.121.1.26 )"),
                new com.unboundid.ldap.sdk.Attribute("objectClasses",
                        "( 1.3.6.1.1.1.2.2 NAME 'posixGroup' SUP top STRUCTURAL MUST ( cn $ gidNumber ) "
                                + "MAY ( userPassword $ memberUid $ description ) )")))));
        config.setListenerConfigs(
                InMemoryListenerConfig.createLDAPConfig("ldap", null, 0,
                        startTls ? serverSsl.createSSLSocketFactory() : null),
                InMemoryListenerConfig.createLDAPSConfig("ldaps", null, 0,
                        serverSsl.createSSLServerSocketFactory(), clientSsl.createSSLSocketFactory()));
        server = new InMemoryDirectoryServer(config);
        server.add("dn: " + BASE, "objectClass: top", "objectClass: domain", "dc: example");
        server.add("dn: ou=people," + BASE, "objectClass: organizationalUnit", "ou: people");
        server.add("dn: ou=contractors," + BASE, "objectClass: organizationalUnit", "ou: contractors");
        person("alice", "people", "alice-pw");
        person("bob", "people", "bob:pw:with:colons");
        person("carol", "people", "carol-pw");
        person("carol", "contractors", "carol-pw");
        server.add("dn: ou=groups," + BASE, "objectClass: organizationalUnit", "ou: groups");
        server.add("dn: cn=analysts,ou=groups," + BASE, "objectClass: groupOfNames", "cn: analysts",
                "member: uid=alice,ou=people," + BASE);
        server.add("dn: cn=bi-users,ou=groups," + BASE, "objectClass: groupOfUniqueNames", "cn: bi-users",
                "description: BI users", "uniqueMember: uid=alice,ou=people," + BASE,
                "uniqueMember: uid=bob,ou=people," + BASE);
        server.add("dn: cn=ops,ou=groups," + BASE, "objectClass: posixGroup", "cn: ops", "gidNumber: 1001",
                "memberUid: bob");
        server.startListening();
    }

    /**
     * Adds a person entry.
     *
     * @param uid user id
     * @param ou organizational unit
     * @param password user password
     * @throws Exception on directory failure
     */
    private void person(String uid, String ou, String password) throws Exception {
        server.add("dn: uid=" + uid + ",ou=" + ou + "," + BASE, "objectClass: inetOrgPerson",
                "uid: " + uid, "cn: " + uid + " " + ou, "sn: " + uid, "userPassword: " + password);
    }

    /**
     * Returns the plain LDAP URL.
     *
     * @param host host name to put in the URL
     * @return URL
     */
    String ldapUrl(String host) {
        return "ldap://" + host + ":" + server.getListenPort("ldap");
    }

    /**
     * Returns the LDAPS URL.
     *
     * @param host host name to put in the URL
     * @return URL
     */
    String ldapsUrl(String host) {
        return "ldaps://" + host + ":" + server.getListenPort("ldaps");
    }

    /**
     * Returns the trust store holding the server certificate.
     *
     * @return PKCS12 file
     */
    Path trustStore() {
        return trustStore;
    }

    /**
     * Base settings for a template-mode provider over StartTLS.
     *
     * @return mutable settings
     */
    Map<String, String> templateSettings() {
        Map<String, String> settings = new HashMap<>();
        settings.put("FLIGHT_LDAP_URL", ldapUrl("localhost"));
        settings.put("FLIGHT_LDAP_START_TLS", "true");
        settings.put("FLIGHT_LDAP_USER_DN_TEMPLATE", "uid={0},ou=people," + BASE);
        settings.put("FLIGHT_LDAP_TRUSTSTORE", trustStore.toString());
        settings.put("FLIGHT_LDAP_TRUSTSTORE_PASSWORD", PASSWORD);
        settings.put("FLIGHT_LDAP_SESSION_SECRET", SESSION_SECRET);
        settings.put("FLIGHT_LDAP_CONNECT_TIMEOUT_MS", "2000");
        return settings;
    }

    /**
     * Base settings for a search-mode provider over LDAPS with the service account.
     *
     * @return mutable settings
     */
    Map<String, String> searchSettings() {
        Map<String, String> settings = templateSettings();
        settings.remove("FLIGHT_LDAP_USER_DN_TEMPLATE");
        settings.remove("FLIGHT_LDAP_START_TLS");
        settings.put("FLIGHT_LDAP_URL", ldapsUrl("localhost"));
        settings.put("FLIGHT_LDAP_USER_SEARCH_BASE", BASE);
        settings.put("FLIGHT_LDAP_BIND_DN", SERVICE_DN);
        settings.put("FLIGHT_LDAP_BIND_PASSWORD", SERVICE_PASSWORD);
        return settings;
    }

    /**
     * Writes the server's certificate and key as PEM files, for a Flight TLS listener.
     *
     * @param dir target directory
     * @return certificate and key paths
     * @throws Exception when the key store cannot be read
     */
    List<Path> pem(Path dir) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStore)) {
            store.load(in, PASSWORD.toCharArray());
        }
        Certificate certificate = store.getCertificate("server");
        PrivateKey key = (PrivateKey) store.getKey("server", PASSWORD.toCharArray());
        Path cert = dir.resolve("flight.crt");
        Path keyFile = dir.resolve("flight.key");
        Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
        Files.writeString(cert, "-----BEGIN CERTIFICATE-----\n"
                + encoder.encodeToString(certificate.getEncoded()) + "\n-----END CERTIFICATE-----\n");
        Files.writeString(keyFile, "-----BEGIN PRIVATE KEY-----\n"
                + encoder.encodeToString(key.getEncoded()) + "\n-----END PRIVATE KEY-----\n");
        return List.of(cert, keyFile);
    }

    /**
     * Runs the JDK's keytool.
     *
     * @param args keytool arguments
     */
    private static void keytool(String... args) {
        List<String> command = new java.util.ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(args));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("keytool failed: " + output);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        server.shutDown(true);
    }
}
