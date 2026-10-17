package pro.surpin.data.arrowflight.server.storage;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A data directory in an S3 bucket: the bucket and the key prefix below which the schemas lie.
 * {@code s3a://} and {@code s3n://} name the same thing and become {@code s3://}.
 *
 * @param bucket bucket name
 * @param prefix key prefix, empty or ending with "/"
 */
public record S3Uri(String bucket, String prefix) {

    /** Schemes that name an S3 bucket. */
    private static final Set<String> SCHEMES = Set.of("s3", "s3a", "s3n");
    /**
     * Bucket names as S3 allows them, plus upper case and underscores, which some older stores
     * accept.
     */
    private static final Pattern BUCKET = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{1,61}[A-Za-z0-9]");
    /**
     * Characters a key may not have. "?" and "#" would end the path of a URL; "*", "[" and "{"
     * are globs; quotes and backslashes have no place in a data directory.
     */
    private static final Pattern REFUSED_KEY_CHARACTERS = Pattern.compile("[?#*\\[\\]{}'\"\\\\\\x00-\\x1f\\x7f]");

    /**
     * Checks a bucket and a prefix.
     *
     * @param bucket bucket name
     * @param prefix key prefix, empty or ending with "/"
     */
    public S3Uri {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(prefix, "prefix");
        if (!BUCKET.matcher(bucket).matches()) {
            throw new IllegalArgumentException("Invalid S3 bucket name: " + bucket);
        }
        if (!prefix.isEmpty()) {
            if (!prefix.endsWith("/")) {
                throw new IllegalArgumentException("S3 prefix must end with '/': " + prefix);
            }
            requireSafeKey(prefix.substring(0, prefix.length() - 1), "data directory");
        }
    }

    /**
     * Tells whether a data directory names an S3 bucket.
     *
     * @param uri data directory as configured
     * @return true for {@code s3://}, {@code s3a://} and {@code s3n://}
     */
    public static boolean isS3(String uri) {
        if (uri == null) {
            return false;
        }
        int separator = uri.trim().indexOf("://");
        return separator > 0
                && SCHEMES.contains(uri.trim().substring(0, separator).toLowerCase(Locale.ROOT));
    }

    /**
     * Parses a data directory such as {@code s3://lake/parquet}.
     *
     * @param uri data directory
     * @return bucket and prefix
     * @throws IllegalArgumentException when it is not an S3 URI or names an unsafe path
     */
    public static S3Uri parse(String uri) {
        if (!isS3(uri)) {
            throw new IllegalArgumentException("Not an S3 URI (s3://bucket/prefix): " + uri);
        }
        String trimmed = uri.trim();
        String rest = trimmed.substring(trimmed.indexOf("://") + 3);
        int slash = rest.indexOf('/');
        String bucket = slash < 0 ? rest : rest.substring(0, slash);
        String path = slash < 0 ? "" : rest.substring(slash + 1);
        while (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if (bucket.contains("@") || bucket.contains(":")) {
            throw new IllegalArgumentException("An S3 URI names a bucket, not a host: " + uri);
        }
        return new S3Uri(bucket, path.isEmpty() ? "" : path + "/");
    }

    /**
     * Turns a path below the data directory into an object key.
     *
     * @param relativePath path below the data directory, "/"-separated, empty for the directory
     * @return object key
     * @throws IllegalArgumentException when the path is absolute, climbs out or has characters
     *         that are not allowed
     */
    public String key(String relativePath) {
        if (relativePath == null || relativePath.isEmpty()) {
            return prefix;
        }
        if (relativePath.startsWith("/")) {
            throw new IllegalArgumentException("Path must be relative to the data directory: "
                    + relativePath);
        }
        String path = relativePath.endsWith("/")
                ? relativePath.substring(0, relativePath.length() - 1) : relativePath;
        requireSafeKey(path, "path");
        return prefix + path;
    }

    /**
     * Names the data directory as configured, without a trailing "/".
     *
     * @return {@code s3://bucket/prefix}
     */
    public String uri() {
        return prefix.isEmpty()
                ? "s3://" + bucket : "s3://" + bucket + "/" + prefix.substring(0, prefix.length() - 1);
    }

    /**
     * Names the data directory as a directory, the form DuckDB's allowed_directories and a
     * secret's scope need.
     *
     * @return {@code s3://bucket/prefix/}
     */
    public String root() {
        return "s3://" + bucket + "/" + prefix;
    }

    /**
     * Names an object the way DuckDB reads it.
     *
     * @param key object key
     * @return {@code s3://bucket/key}
     */
    public String objectUri(String key) {
        return "s3://" + bucket + "/" + key;
    }

    /**
     * Refuses a "/"-separated key that is empty, climbs out or has refused characters.
     *
     * @param path key or part of a key, without leading or trailing "/"
     * @param what what the path is, for the error
     */
    private static void requireSafeKey(String path, String what) {
        if (REFUSED_KEY_CHARACTERS.matcher(path).find()) {
            throw new IllegalArgumentException("Invalid character in S3 " + what + ": " + path);
        }
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)
                    || segment.toLowerCase(Locale.ROOT).contains("%2f")
                    || segment.contains("%2e") || segment.contains("%2E")) {
                throw new IllegalArgumentException("Invalid S3 " + what + ": " + path);
            }
        }
    }
}
