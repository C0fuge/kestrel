package pro.surpin.data.arrowflight.plugin.lakefs;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A lakeFS data directory, {@code lakefs://repository/ref/prefix}: the repository is the S3
 * gateway's bucket, the ref (branch, tag or commit id) the first path segment.
 *
 * @param repository repository name
 * @param ref branch, tag or commit id
 * @param prefix path below the ref, without leading or trailing "/", empty for the ref's root
 */
public record LakeFsUri(String repository, String ref, String prefix) {

    /** URI scheme of lakeFS data directories. */
    public static final String SCHEME = "lakefs";

    /** lakeFS repository names: lower case letters, digits and dashes, 3 to 63 characters. */
    private static final Pattern REPOSITORY = Pattern.compile("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]");

    /** Branch, tag and commit names this plugin accepts; no "/", "~", "^" or "@" expressions. */
    private static final Pattern REF = Pattern.compile("[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}");

    /**
     * Checks the parts.
     *
     * @param repository repository name
     * @param ref branch, tag or commit id
     * @param prefix path below the ref
     */
    public LakeFsUri {
        if (repository == null || !REPOSITORY.matcher(repository).matches()) {
            throw new IllegalArgumentException("Invalid lakeFS repository name: " + repository);
        }
        requireRef(ref);
        prefix = prefix == null ? "" : prefix;
        if (prefix.startsWith("/") || prefix.endsWith("/") || prefix.contains("//")) {
            throw new IllegalArgumentException("Invalid path in lakeFS data directory: " + prefix);
        }
    }

    /**
     * Refuses a ref this plugin does not accept.
     *
     * @param ref branch, tag or commit id
     * @return the ref
     */
    static String requireRef(String ref) {
        if (ref == null || !REF.matcher(ref).matches() || ref.contains("..")) {
            throw new IllegalArgumentException("Invalid lakeFS ref (branch, tag or commit id): "
                    + ref);
        }
        return ref;
    }

    /**
     * Parses {@code lakefs://repository/ref[/prefix]}.
     *
     * @param uri data directory
     * @return parsed URI
     * @throws IllegalArgumentException when the URI is not a lakeFS data directory
     */
    public static LakeFsUri parse(String uri) {
        String trimmed = uri == null ? "" : uri.trim();
        int separator = trimmed.indexOf("://");
        if (separator <= 0 || !SCHEME.equals(trimmed.substring(0, separator).toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException(
                    "Not a lakeFS URI (lakefs://repository/branch/prefix): " + uri);
        }
        String rest = trimmed.substring(separator + 3);
        while (rest.endsWith("/")) {
            rest = rest.substring(0, rest.length() - 1);
        }
        String[] parts = rest.split("/", 3);
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "A lakeFS data directory names a repository and a branch or commit"
                    + " (lakefs://repository/branch/prefix): " + uri);
        }
        return new LakeFsUri(parts[0], parts[1], parts.length > 2 ? parts[2] : "");
    }

    /**
     * Returns the same directory at another ref.
     *
     * @param otherRef branch, tag or commit id
     * @return directory at that ref
     */
    public LakeFsUri at(String otherRef) {
        return new LakeFsUri(repository, otherRef, prefix);
    }

    /**
     * Names the directory on the S3 gateway.
     *
     * @return {@code s3://repository/ref[/prefix]}
     */
    public String s3Uri() {
        return "s3://" + repository + "/" + ref + (prefix.isEmpty() ? "" : "/" + prefix);
    }

    /**
     * Names the directory as configured.
     *
     * @return {@code lakefs://repository/ref[/prefix]}
     */
    public String uri() {
        return SCHEME + "://" + repository + "/" + ref + (prefix.isEmpty() ? "" : "/" + prefix);
    }
}
