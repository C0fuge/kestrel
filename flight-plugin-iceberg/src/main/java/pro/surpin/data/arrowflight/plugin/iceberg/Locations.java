package pro.surpin.data.arrowflight.plugin.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Names Iceberg file locations the way the server's stores and DuckDB name them: a local file as
 * a plain absolute path ({@code file:/x}, {@code file:///x} and {@code /x} alike), an S3 object
 * as {@code s3://} ({@code s3a://} and {@code s3n://} alike, as Spark's S3A writes them), every
 * other URI as written.
 */
final class Locations {

    private Locations() {
    }

    /**
     * Normalises a location.
     *
     * @param location location from Iceberg metadata or configuration
     * @return plain path for a local file, {@code s3://} for an S3 object, the location
     *         otherwise; without a trailing slash unless it is the only character
     */
    static String normalize(String location) {
        String result = location.strip();
        String lower = result.toLowerCase(Locale.ROOT);
        if (lower.startsWith("s3a://") || lower.startsWith("s3n://")) {
            result = "s3://" + result.substring("s3a://".length());
        } else if (lower.startsWith("file://")) {
            result = result.substring("file://".length());
        } else if (lower.startsWith("file:")) {
            result = result.substring("file:".length());
        }
        if (result.startsWith("//") && !result.contains("://")) {
            // file:////x and the like
            while (result.startsWith("//")) {
                result = result.substring(1);
            }
        }
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    /**
     * Tells whether a location lies under a root (or is the root).
     *
     * @param location normalised location
     * @param root normalised root
     * @return true when it does
     */
    static boolean under(String location, String root) {
        String prefix = root.endsWith("/") ? root : root + "/";
        return location.equals(root) || location.startsWith(prefix);
    }

    /**
     * Tells whether a location lies under one of some roots.
     *
     * @param location normalised location
     * @param roots normalised roots
     * @return true when it does
     */
    static boolean underAny(String location, List<String> roots) {
        for (String root : roots) {
            if (under(location, root)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Splits a comma separated list of roots and normalises each.
     *
     * @param value list, may be blank
     * @return roots
     */
    static List<String> roots(String value) {
        List<String> result = new ArrayList<>();
        if (value == null) {
            return result;
        }
        for (String part : value.split(",")) {
            if (!part.isBlank()) {
                result.add(normalize(part));
            }
        }
        return result;
    }
}
