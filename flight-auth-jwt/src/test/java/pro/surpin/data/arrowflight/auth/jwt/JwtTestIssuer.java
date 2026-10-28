package pro.surpin.data.arrowflight.auth.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.Curve;
import com.nimbusds.jose.jwk.ECKey;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.ECKeyGenerator;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A token issuer for tests: RSA and EC signing keys, signed tokens, the public keys as a JWKS file
 * and over HTTPS (localhost, keytool-made certificate), with key rotation.
 */
public final class JwtTestIssuer implements AutoCloseable {
    /** Issuer of the tokens. */
    public static final String ISSUER = "https://idp.test/realms/data";
    /** Audience of the tokens. */
    public static final String AUDIENCE = "flight";
    static final String PASSWORD = "store-pass";
    private final Path dir;
    private final Path keyStore;
    private final Path trustStore;
    private final List<JWK> published = new ArrayList<>();
    private final AtomicInteger fetches = new AtomicInteger();
    private RSAKey rsa;
    private final ECKey ec;
    private HttpsServer server;

    /**
     * Creates the keys and the TLS material.
     *
     * @param dir scratch directory
     * @throws Exception when keys cannot be made
     */
    JwtTestIssuer(Path dir) throws Exception {
        this.dir = dir;
        rsa = new RSAKeyGenerator(2048).keyID("rsa-1").generate();
        ec = new ECKeyGenerator(Curve.P_256).keyID("ec-1").generate();
        published.add(rsa.toPublicJWK());
        published.add(ec.toPublicJWK());
        keyStore = dir.resolve("tls.p12");
        trustStore = dir.resolve("trust.p12");
        Path cert = dir.resolve("tls.crt");
        keytool("-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=jwt-test", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2",
                "-keystore", keyStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
        keytool("-exportcert", "-alias", "server", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", cert.toString());
        keytool("-importcert", "-noprompt", "-alias", "ca", "-file", cert.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD);
    }

    /**
     * Creates an issuer for tests in other packages.
     *
     * @param dir scratch directory
     * @return issuer
     * @throws Exception when keys cannot be made
     */
    public static JwtTestIssuer create(Path dir) throws Exception {
        return new JwtTestIssuer(dir);
    }

    /**
     * Settings of a provider reading the public keys from a file.
     *
     * @return mutable settings
     * @throws IOException when the file cannot be written
     */
    public Map<String, String> fileSettings() throws IOException {
        Path jwks = dir.resolve("jwks.json");
        Files.writeString(jwks, new JWKSet(published).toString(true));
        Map<String, String> settings = new HashMap<>();
        settings.put("FLIGHT_JWT_JWKS_FILE", jwks.toString());
        settings.put("FLIGHT_JWT_ISSUER", ISSUER);
        settings.put("FLIGHT_JWT_AUDIENCE", AUDIENCE);
        return settings;
    }

    /**
     * Starts the HTTPS key set endpoint and returns settings using it.
     *
     * @return mutable settings
     * @throws Exception when the server cannot start
     */
    Map<String, String> httpsSettings() throws Exception {
        if (server == null) {
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(keyStore)) {
                store.load(in, PASSWORD.toCharArray());
            }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, PASSWORD.toCharArray());
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), null, null);
            server = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(context));
            server.createContext("/jwks", exchange -> {
                fetches.incrementAndGet();
                byte[] body;
                synchronized (published) {
                    body = new JWKSet(published).toString(true).getBytes(StandardCharsets.UTF_8);
                }
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            });
            server.start();
        }
        Map<String, String> settings = fileSettings();
        settings.remove("FLIGHT_JWT_JWKS_FILE");
        settings.put("FLIGHT_JWT_JWKS_URL", "https://localhost:" + server.getAddress().getPort() + "/jwks");
        settings.put("FLIGHT_JWT_TRUSTSTORE", trustStore.toString());
        settings.put("FLIGHT_JWT_TRUSTSTORE_PASSWORD", PASSWORD);
        return settings;
    }

    /**
     * Returns how often the key set was fetched over HTTPS.
     *
     * @return fetch count
     */
    int fetches() {
        return fetches.get();
    }

    /**
     * Replaces the RSA key with a new one under a new key id and publishes only the new one.
     *
     * @param keyId new key id
     * @throws Exception when the key cannot be made
     */
    void rotateRsa(String keyId) throws Exception {
        rsa = new RSAKeyGenerator(2048).keyID(keyId).generate();
        synchronized (published) {
            published.clear();
            published.add(rsa.toPublicJWK());
            published.add(ec.toPublicJWK());
        }
    }

    /**
     * Claims of a valid token for a user, valid for ten minutes.
     *
     * @param subject subject
     * @return claims builder
     */
    public static JWTClaimsSet.Builder claims(String subject) {
        Instant now = Instant.now();
        return new JWTClaimsSet.Builder().issuer(ISSUER).audience(AUDIENCE).subject(subject)
                .issueTime(Date.from(now)).expirationTime(Date.from(now.plusSeconds(600)));
    }

    /**
     * Signs claims with the RSA key (RS256).
     *
     * @param claims claims
     * @return compact token
     * @throws Exception on signing failure
     */
    public String rs256(JWTClaimsSet claims) throws Exception {
        return rs256(claims, JOSEObjectType.JWT);
    }

    /**
     * Signs claims with the RSA key (RS256) under a given token type.
     *
     * @param claims claims
     * @param type header {@code typ}
     * @return compact token
     * @throws Exception on signing failure
     */
    String rs256(JWTClaimsSet claims, JOSEObjectType type) throws Exception {
        return sign(new RSASSASigner(rsa), new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsa.getKeyID())
                .type(type).build(), claims);
    }

    /**
     * Signs claims with the EC key (ES256).
     *
     * @param claims claims
     * @return compact token
     * @throws Exception on signing failure
     */
    String es256(JWTClaimsSet claims) throws Exception {
        return sign(new ECDSASigner(ec), new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(ec.getKeyID()).build(),
                claims);
    }

    /**
     * Signs claims with a given signer and header.
     *
     * @param signer signer
     * @param header header
     * @param claims claims
     * @return compact token
     * @throws Exception on signing failure
     */
    static String sign(JWSSigner signer, JWSHeader header, JWTClaimsSet claims) throws Exception {
        SignedJWT jwt = new SignedJWT(header, claims);
        jwt.sign(signer);
        return jwt.serialize();
    }

    /**
     * Writes the TLS certificate and key as PEM files, for a Flight TLS listener.
     *
     * @return certificate and key paths
     * @throws Exception when the key store cannot be read
     */
    public List<Path> pem() throws Exception {
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
     * Applies a change to fresh valid claims and signs them with RS256.
     *
     * @param subject subject
     * @param change claims change
     * @return compact token
     * @throws Exception on signing failure
     */
    public String token(String subject, Consumer<JWTClaimsSet.Builder> change) throws Exception {
        JWTClaimsSet.Builder builder = claims(subject);
        change.accept(builder);
        return rs256(builder.build());
    }

    /**
     * Runs the JDK's keytool.
     *
     * @param args keytool arguments
     */
    private static void keytool(String... args) {
        List<String> command = new ArrayList<>();
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
        if (server != null) {
            server.stop(0);
        }
    }
}
