package pro.surpin.data.arrowflight.auth.jwt;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.KeySourceException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.DefaultResourceRetriever;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Verifies a JWT's signature against the issuer's key set and its issuer, audience, expiry and
 * not-before claims, and reads the caller's identity and groups from it.
 */
final class JwtVerifier {
    /** Most groups one token may name; more is refused instead of cut short. */
    static final int MAX_GROUPS = 1000;
    private static final Pattern IDENTITY = Pattern.compile("[\\p{L}\\p{N}._@-]{1,256}");
    private static final int MAX_JWKS_BYTES = 512 * 1024;
    private final DefaultJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
    private final String userClaim;
    private final String[] groupsPath;
    private final long clockSkewMillis;

    /**
     * A verified token.
     *
     * @param identity the caller's identity
     * @param groups the groups the token names, empty without a groups claim
     * @param expiresAtMillis when the token stops being accepted, clock skew included
     */
    record Verified(String identity, Set<String> groups, long expiresAtMillis) {
        /**
         * Copies the groups.
         */
        Verified {
            groups = Set.copyOf(groups);
        }
    }

    /** A token that is malformed, badly signed, expired or meant for another service. */
    static final class InvalidTokenException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message reason, without token content
         * @param cause underlying failure, or null
         */
        InvalidTokenException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** The issuer's key set could not be fetched, so no token can be checked. */
    static final class KeysUnavailableException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates the exception.
         *
         * @param message reason
         * @param cause underlying failure
         */
        KeysUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Creates the verifier.
     *
     * @param settings provider settings
     */
    JwtVerifier(JwtSettings settings) {
        this(settings, keySource(settings));
    }

    /**
     * Creates the verifier with a given key source.
     *
     * @param settings provider settings
     * @param keys the issuer's keys
     */
    JwtVerifier(JwtSettings settings, JWKSource<SecurityContext> keys) {
        this.userClaim = settings.userClaim();
        this.groupsPath = settings.groupsClaim() == null ? null : settings.groupsClaim().split("\\.");
        this.clockSkewMillis = settings.clockSkewSeconds() * 1000L;
        processor.setJWSTypeVerifier(new DefaultJOSEObjectTypeVerifier<>(JOSEObjectType.JWT,
                new JOSEObjectType("at+jwt"), null));
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(settings.algorithms(), keys));
        DefaultJWTClaimsVerifier<SecurityContext> claims = new DefaultJWTClaimsVerifier<>(
                settings.audiences(), new JWTClaimsSet.Builder().issuer(settings.issuer()).build(),
                Set.of("iss", "aud", "exp", settings.userClaim()), null);
        claims.setMaxClockSkew(settings.clockSkewSeconds());
        processor.setJWTClaimsSetVerifier(claims);
    }

    /**
     * Verifies a token.
     *
     * @param token compact serialized JWT
     * @return identity, groups and acceptance end of the token
     * @throws InvalidTokenException when the token is not acceptable
     * @throws KeysUnavailableException when the issuer's keys cannot be fetched
     */
    Verified verify(String token) throws InvalidTokenException, KeysUnavailableException {
        JWTClaimsSet claims;
        try {
            claims = processor.process(token, null);
        } catch (KeySourceException e) {
            throw new KeysUnavailableException("JWKS not available: " + e.getMessage(), e);
        } catch (ParseException | BadJOSEException | JOSEException e) {
            throw new InvalidTokenException(e.getMessage(), e);
        }
        String identity;
        try {
            identity = claims.getStringClaim(userClaim);
        } catch (ParseException e) {
            throw new InvalidTokenException("claim " + userClaim + " is not a string", e);
        }
        if (identity == null || !IDENTITY.matcher(identity).matches()) {
            throw new InvalidTokenException("claim " + userClaim + " is not a valid identity (letters, digits, "
                    + "'._@-', at most 256)", null);
        }
        return new Verified(identity, groups(claims), claims.getExpirationTime().getTime() + clockSkewMillis);
    }

    /**
     * Reads the groups claim: an array of strings or one string, missing meaning no groups.
     *
     * @param claims verified claims
     * @return groups
     * @throws InvalidTokenException when the claim has another shape or too many entries
     */
    private Set<String> groups(JWTClaimsSet claims) throws InvalidTokenException {
        if (groupsPath == null) {
            return Set.of();
        }
        Object value = claims.getClaim(groupsPath[0]);
        for (int i = 1; i < groupsPath.length && value != null; i++) {
            if (!(value instanceof Map<?, ?> object)) {
                throw new InvalidTokenException("groups claim is not an object at " + groupsPath[i - 1], null);
            }
            value = object.get(groupsPath[i]);
        }
        if (value == null) {
            return Set.of();
        }
        if (value instanceof String group) {
            return Set.of(group);
        }
        if (!(value instanceof Collection<?> list)) {
            throw new InvalidTokenException("groups claim is neither a string nor an array", null);
        }
        if (list.size() > MAX_GROUPS) {
            throw new InvalidTokenException("groups claim has more than " + MAX_GROUPS + " entries", null);
        }
        Set<String> groups = new LinkedHashSet<>();
        for (Object group : list) {
            if (!(group instanceof String name)) {
                throw new InvalidTokenException("groups claim holds a non-string entry", null);
            }
            groups.add(name);
        }
        return groups;
    }

    /**
     * Builds the key source: a fixed key set file, or a cached HTTPS key set that is fetched again
     * when it ages out or a token names an unknown key (rate limited).
     *
     * @param settings provider settings
     * @return key source
     */
    private static JWKSource<SecurityContext> keySource(JwtSettings settings) {
        if (settings.jwksFile() != null) {
            try {
                return new ImmutableJWKSet<>(JWKSet.load(settings.jwksFile().toFile()).toPublicJWKSet());
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot read FLIGHT_JWT_JWKS_FILE", e);
            } catch (ParseException e) {
                throw new IllegalArgumentException("FLIGHT_JWT_JWKS_FILE is not a JWK set: " + e.getMessage(), e);
            }
        }
        DefaultResourceRetriever retriever = new DefaultResourceRetriever(settings.connectTimeoutMillis(),
                settings.readTimeoutMillis(), MAX_JWKS_BYTES, true, socketFactory(settings));
        return JWKSourceBuilder.create(settings.jwksUrl(), retriever)
                .cache(settings.jwksCacheMillis(), settings.readTimeoutMillis() + settings.connectTimeoutMillis())
                .rateLimited(settings.jwksRefreshLimitMillis())
                .build();
    }

    /**
     * Builds the TLS socket factory for key set fetches.
     *
     * @param settings provider settings
     * @return socket factory, or null for the JVM default
     */
    private static SSLSocketFactory socketFactory(JwtSettings settings) {
        if (settings.trustStore() == null) {
            return null;
        }
        try (InputStream in = Files.newInputStream(settings.trustStore())) {
            KeyStore store = KeyStore.getInstance(settings.trustStoreType());
            store.load(in, settings.trustStorePassword() == null ? null
                    : settings.trustStorePassword().toCharArray());
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trust.getTrustManagers(), null);
            return context.getSocketFactory();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read FLIGHT_JWT_TRUSTSTORE", e);
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("Unusable FLIGHT_JWT_TRUSTSTORE: " + e.getMessage(), e);
        }
    }
}
