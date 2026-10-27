package pro.surpin.data.arrowflight.plugin.s3;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Checks AWS Signature Version 4 on requests to the loopback endpoint of the S3 read cache, so
 * that only a client holding a route's secret (DuckDB, through its secret) can read through it.
 */
final class S3SigV4 {

    private static final String ALGORITHM = "AWS4-HMAC-SHA256";
    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    /** Largest accepted difference between the request's time and ours. */
    private static final Duration SKEW = Duration.ofMinutes(15);

    private S3SigV4() {
    }

    /**
     * One parsed {@code Authorization} header.
     *
     * @param keyId access key ID of the credential
     * @param scope credential scope, {@code date/region/service/aws4_request}
     * @param date date of the scope
     * @param signedHeaders lower-case names of the signed headers, as listed
     * @param signature hex signature
     */
    record Authorization(String keyId, String scope, String date, List<String> signedHeaders, String signature) {
    }

    /**
     * Parses an {@code Authorization} header of SigV4.
     *
     * @param header header value, or null
     * @return parsed header, or empty when it is missing or not SigV4
     */
    static Optional<Authorization> parse(String header) {
        if (header == null || !header.startsWith(ALGORITHM + " ")) {
            return Optional.empty();
        }
        String credential = null;
        String signed = null;
        String signature = null;
        for (String part : header.substring(ALGORITHM.length() + 1).split(",")) {
            String p = part.trim();
            if (p.startsWith("Credential=")) {
                credential = p.substring("Credential=".length());
            } else if (p.startsWith("SignedHeaders=")) {
                signed = p.substring("SignedHeaders=".length());
            } else if (p.startsWith("Signature=")) {
                signature = p.substring("Signature=".length());
            }
        }
        if (credential == null || signed == null || signature == null) {
            return Optional.empty();
        }
        String[] scope = credential.split("/");
        if (scope.length != 5 || !"aws4_request".equals(scope[4]) || scope[0].isEmpty()) {
            return Optional.empty();
        }
        List<String> headers = List.of(signed.toLowerCase(Locale.ROOT).split(";"));
        return Optional.of(new Authorization(scope[0], credential.substring(scope[0].length() + 1),
                scope[1], headers, signature.toLowerCase(Locale.ROOT)));
    }

    /**
     * Checks the signature of a request.
     *
     * @param method HTTP method
     * @param rawPath path as sent, still encoded
     * @param rawQuery query as sent, or null
     * @param headers header value by name (any case), null when absent
     * @param secretOf secret key of an access key ID, null for an unknown one
     * @param now current time
     * @return access key ID of the valid signature, or empty when the request is not signed
     *         with a known key, is too old or too new, or does not match its signature
     */
    static Optional<String> verify(String method, String rawPath, String rawQuery,
            Function<String, String> headers, Function<String, String> secretOf, Instant now) {
        Optional<Authorization> parsed = parse(headers.apply("authorization"));
        if (parsed.isEmpty()) {
            return Optional.empty();
        }
        Authorization auth = parsed.get();
        String secret = secretOf.apply(auth.keyId());
        String amzDate = headers.apply("x-amz-date");
        String payloadHash = headers.apply("x-amz-content-sha256");
        if (secret == null || amzDate == null || payloadHash == null || !amzDate.startsWith(auth.date())
                || !auth.signedHeaders().contains("host") || !auth.signedHeaders().contains("x-amz-date")) {
            return Optional.empty();
        }
        try {
            Instant sent = LocalDateTime.parse(amzDate, AMZ_DATE).toInstant(ZoneOffset.UTC);
            if (Duration.between(sent, now).abs().compareTo(SKEW) > 0) {
                return Optional.empty();
            }
        } catch (DateTimeParseException e) {
            return Optional.empty();
        }
        StringBuilder canonical = new StringBuilder();
        canonical.append(method).append('\n').append(rawPath.isEmpty() ? "/" : rawPath).append('\n')
                .append(canonicalQuery(rawQuery)).append('\n');
        for (String name : auth.signedHeaders()) {
            String value = headers.apply(name);
            if (value == null) {
                return Optional.empty();
            }
            canonical.append(name).append(':').append(value.trim().replaceAll("\\s+", " ")).append('\n');
        }
        canonical.append('\n').append(String.join(";", auth.signedHeaders())).append('\n').append(payloadHash);
        String toSign = ALGORITHM + "\n" + amzDate + "\n" + auth.scope() + "\n" + hex(sha256(canonical.toString()));
        String[] scope = auth.scope().split("/");
        byte[] key = hmac(("AWS4" + secret).getBytes(StandardCharsets.UTF_8), scope[0]);
        key = hmac(key, scope[1]);
        key = hmac(key, scope[2]);
        key = hmac(key, scope[3]);
        byte[] expected = hex(hmac(key, toSign)).getBytes(StandardCharsets.US_ASCII);
        boolean valid = MessageDigest.isEqual(expected, auth.signature().getBytes(StandardCharsets.US_ASCII));
        return valid ? Optional.of(auth.keyId()) : Optional.empty();
    }

    /**
     * Builds the canonical query string: parameters sorted by name, then value, as sent.
     *
     * @param rawQuery query as sent, or null
     * @return canonical query, empty for none
     */
    private static String canonicalQuery(String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) {
            return "";
        }
        List<String> pairs = new ArrayList<>();
        for (String pair : rawQuery.split("&")) {
            pairs.add(pair.indexOf('=') < 0 ? pair + "=" : pair);
        }
        pairs.sort(null);
        return String.join("&", pairs);
    }

    /**
     * Hashes a text with SHA-256.
     *
     * @param text UTF-8 text
     * @return digest
     */
    private static byte[] sha256(String text) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /**
     * Computes an HMAC-SHA256.
     *
     * @param key key
     * @param data UTF-8 text
     * @return MAC
     */
    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HmacSHA256 not available", e);
        }
    }

    /**
     * Formats bytes as lower-case hex.
     *
     * @param bytes bytes
     * @return hex text
     */
    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }
}
