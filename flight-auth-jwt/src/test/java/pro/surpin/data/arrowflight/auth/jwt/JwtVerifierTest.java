package pro.surpin.data.arrowflight.auth.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.PlainJWT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Token checks: signatures, claims, identity and groups, key sets from a file and over HTTPS. */
@Tag("unit")
class JwtVerifierTest {
    @TempDir
    static Path dir;
    private static JwtTestIssuer issuer;

    @BeforeAll
    static void start() throws Exception {
        issuer = new JwtTestIssuer(dir);
    }

    @AfterAll
    static void stop() {
        issuer.close();
    }

    /**
     * Builds a verifier over the issuer's key set file.
     *
     * @param extra additional settings
     * @return verifier
     * @throws Exception when the settings are refused
     */
    private static JwtVerifier verifier(String... extra) throws Exception {
        Map<String, String> settings = issuer.fileSettings();
        for (int i = 0; i < extra.length; i += 2) {
            settings.put(extra[i], extra[i + 1]);
        }
        return new JwtVerifier(JwtSettings.fromEnvironment(settings));
    }

    /**
     * Asserts that a token is refused as invalid.
     *
     * @param verifier verifier
     * @param token token
     */
    private static void refused(JwtVerifier verifier, String token) {
        assertThrows(JwtVerifier.InvalidTokenException.class, () -> verifier.verify(token));
    }

    /** RS256 and ES256 tokens are accepted; identity from sub or a configured claim. */
    @Test
    void acceptsValidTokens() throws Exception {
        JwtVerifier verifier = verifier();
        JwtVerifier.Verified rsa = verifier.verify(issuer.token("alice", b -> { }));
        assertEquals("alice", rsa.identity());
        assertEquals(Set.of(), rsa.groups());
        assertEquals("bob@corp.example", verifier.verify(issuer.es256(JwtTestIssuer.claims("bob@corp.example")
                .build())).identity());
        String typed = JwtTestIssuer.sign(new RSASSASigner(rsaKey()), new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID("rsa-1").type(new JOSEObjectType("at+jwt")).build(), JwtTestIssuer.claims("carol").build());
        refused(verifier, typed); // signed by another key under the published kid
        assertEquals("carol", verifier.verify(issuer.rs256(JwtTestIssuer.claims("carol").build(),
                new JOSEObjectType("at+jwt"))).identity());
        JwtVerifier byName = verifier("FLIGHT_JWT_USER_CLAIM", "preferred_username");
        assertEquals("dave", byName.verify(issuer.token("f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
                b -> b.claim("preferred_username", "dave"))).identity());
        refused(byName, issuer.token("x", b -> { }));
        assertEquals("erin", verifier("FLIGHT_JWT_AUDIENCE", "other, flight").verify(issuer.token("erin",
                b -> b.audience(List.of("account", "flight")))).identity());
    }

    /** Groups from an array, one string, a nested path; missing means none; other shapes are refused. */
    @Test
    void readsGroups() throws Exception {
        JwtVerifier flat = verifier("FLIGHT_JWT_GROUPS_CLAIM", "groups");
        assertEquals(Set.of("analysts", "bi-users"), flat.verify(issuer.token("alice",
                b -> b.claim("groups", List.of("analysts", "bi-users", "analysts")))).groups());
        assertEquals(Set.of("ops"), flat.verify(issuer.token("bob", b -> b.claim("groups", "ops"))).groups());
        assertEquals(Set.of(), flat.verify(issuer.token("carol", b -> { })).groups());
        refused(flat, issuer.token("x", b -> b.claim("groups", 7)));
        refused(flat, issuer.token("x", b -> b.claim("groups", List.of("a", 1))));
        List<String> many = new ArrayList<>();
        for (int i = 0; i <= JwtVerifier.MAX_GROUPS; i++) {
            many.add("g" + i);
        }
        refused(flat, issuer.token("x", b -> b.claim("groups", many)));
        JwtVerifier nested = verifier("FLIGHT_JWT_GROUPS_CLAIM", "realm_access.roles");
        assertEquals(Set.of("reader"), nested.verify(issuer.token("alice",
                b -> b.claim("realm_access", Map.of("roles", List.of("reader"))))).groups());
        assertEquals(Set.of(), nested.verify(issuer.token("alice", b -> b.claim("realm_access", Map.of())))
                .groups());
        refused(nested, issuer.token("x", b -> b.claim("realm_access", "reader")));
    }

    /** Expiry, not-before, issuer, audience and required claims are enforced, with clock skew; a numeric sub
     * is read as text (as the JWT library parses it), other non-string identity claims are refused. */
    @Test
    void refusesBadClaims() throws Exception {
        JwtVerifier verifier = verifier("FLIGHT_JWT_CLOCK_SKEW_SECONDS", "30");
        Instant now = Instant.now();
        refused(verifier, issuer.token("a", b -> b.expirationTime(Date.from(now.minusSeconds(60)))));
        assertEquals("a", verifier.verify(issuer.token("a", b -> b.expirationTime(Date.from(
                now.minusSeconds(5))))).identity());
        refused(verifier, issuer.token("a", b -> b.notBeforeTime(Date.from(now.plusSeconds(120)))));
        refused(verifier, issuer.token("a", b -> b.expirationTime(null)));
        refused(verifier, issuer.token("a", b -> b.issuer("https://evil.test")));
        refused(verifier, issuer.token("a", b -> b.issuer(null)));
        refused(verifier, issuer.token("a", b -> b.audience("other")));
        refused(verifier, issuer.token("a", b -> b.audience((String) null)));
        refused(verifier, issuer.token(null, b -> { }));
        refused(verifier, issuer.token("a b", b -> { }));
        refused(verifier, issuer.token("a", b -> b.subject("x".repeat(257))));
        assertEquals("42", verifier.verify(issuer.token("a", b -> b.claim("sub", 42))).identity());
        JwtVerifier byName = verifier("FLIGHT_JWT_USER_CLAIM", "upn");
        refused(byName, issuer.token("a", b -> b.claim("upn", 42)));
        refused(byName, issuer.token("a", b -> b.claim("upn", List.of("a"))));
    }

    /** Unsigned, HMAC, tampered, unknown-key, wrong-type and disallowed-algorithm tokens are refused. */
    @Test
    void refusesBadSignatures() throws Exception {
        JwtVerifier verifier = verifier();
        refused(verifier, new PlainJWT(JwtTestIssuer.claims("alice").build()).serialize());
        byte[] secret = "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
        refused(verifier, JwtTestIssuer.sign(new MACSigner(secret), new JWSHeader.Builder(JWSAlgorithm.HS256)
                .keyID("rsa-1").build(), JwtTestIssuer.claims("alice").build()));
        String good = issuer.token("alice", b -> { });
        String[] parts = good.split("\\.");
        String forged = JwtTestIssuer.claims("root").build().toString();
        refused(verifier, parts[0] + "." + java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                forged.getBytes(StandardCharsets.UTF_8)) + "." + parts[2]);
        refused(verifier, JwtTestIssuer.sign(new RSASSASigner(rsaKey()), new JWSHeader.Builder(
                JWSAlgorithm.RS256).keyID("unknown").build(), JwtTestIssuer.claims("alice").build()));
        refused(verifier, JwtTestIssuer.sign(new RSASSASigner(rsaKey()), new JWSHeader.Builder(
                JWSAlgorithm.RS256).keyID("rsa-1").type(JOSEObjectType.JOSE).build(),
                JwtTestIssuer.claims("alice").build()));
        refused(verifier, "not-a-token");
        refused(verifier, "");
        JwtVerifier rsaOnly = verifier("FLIGHT_JWT_ALGORITHMS", "RS256");
        assertEquals("alice", rsaOnly.verify(good).identity());
        refused(rsaOnly, issuer.es256(JwtTestIssuer.claims("alice").build()));
    }

    /** Keys come over HTTPS, are cached, and a new key id after a rotation triggers one more fetch. */
    @Test
    void fetchesRotatedKeysOverHttps() throws Exception {
        try (JwtTestIssuer rotating = new JwtTestIssuer(java.nio.file.Files.createDirectories(dir.resolve("r")))) {
            Map<String, String> settings = rotating.httpsSettings();
            settings.put("FLIGHT_JWT_JWKS_REFRESH_LIMIT_SECONDS", "1");
            JwtVerifier verifier = new JwtVerifier(JwtSettings.fromEnvironment(settings));
            assertEquals("alice", verifier.verify(rotating.token("alice", b -> { })).identity());
            assertEquals("bob", verifier.verify(rotating.es256(JwtTestIssuer.claims("bob").build())).identity());
            assertEquals(1, rotating.fetches());
            String old = rotating.token("alice", b -> { });
            Thread.sleep(1_100);
            rotating.rotateRsa("rsa-2");
            assertEquals("carol", verifier.verify(rotating.token("carol", b -> { })).identity());
            assertEquals(2, rotating.fetches());
            assertThrows(JwtVerifier.InvalidTokenException.class, () -> verifier.verify(old));
        }
    }

    /** An untrusted certificate or an unreachable key set is reported as unavailable, not as a bad token. */
    @Test
    void unreachableKeysAreUnavailable() throws Exception {
        try (JwtTestIssuer other = new JwtTestIssuer(java.nio.file.Files.createDirectories(dir.resolve("u")))) {
            Map<String, String> settings = other.httpsSettings();
            settings.remove("FLIGHT_JWT_TRUSTSTORE");
            settings.remove("FLIGHT_JWT_TRUSTSTORE_PASSWORD");
            JwtVerifier untrusted = new JwtVerifier(JwtSettings.fromEnvironment(settings));
            String token = other.token("alice", b -> { });
            JwtVerifier.KeysUnavailableException e = assertThrows(JwtVerifier.KeysUnavailableException.class,
                    () -> untrusted.verify(token));
            assertTrue(e.getMessage().contains("JWKS"), e.getMessage());
            Map<String, String> down = other.httpsSettings();
            down.put("FLIGHT_JWT_JWKS_URL", "https://localhost:1/jwks");
            down.put("FLIGHT_JWT_CONNECT_TIMEOUT_MS", "500");
            JwtVerifier unreachable = new JwtVerifier(JwtSettings.fromEnvironment(down));
            assertThrows(JwtVerifier.KeysUnavailableException.class, () -> unreachable.verify(token));
        }
    }

    /**
     * Makes a fresh RSA key under the id the issuer publishes.
     *
     * @return key
     * @throws Exception on key generation failure
     */
    private static RSAKey rsaKey() throws Exception {
        return new RSAKeyGenerator(2048).keyID("rsa-1").generate();
    }
}
