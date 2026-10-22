package pro.surpin.data.arrowflight.plugin.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Names metastore locations the way the server's stores and DuckDB name them, and tells whether
 * a location lies under the catalog's roots (the same rules as the server's own check).
 */
final class HiveLocations {

    private HiveLocations() {
    }

    /**
     * Normalises a location: {@code s3a://} and {@code s3n://} become {@code s3://} (the
     * server's S3 store), a local file a plain path, trailing slashes go.
     *
     * @param location location as the metastore stores it
     * @return normalised location
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
        if (!result.contains("://")) {
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
     * Reads a comma separated list of roots.
     *
     * @param text roots
     * @return normalised roots
     */
    static List<String> roots(String text) {
        List<String> result = new ArrayList<>();
        for (String part : text.split(",")) {
            if (!part.isBlank()) {
                result.add(normalize(part));
            }
        }
        return List.copyOf(result);
    }

    /**
     * Tells whether a location lies under one of some roots; a location with a "." or ".."
     * segment never does.
     *
     * @param location normalised location
     * @param roots normalised roots
     * @return true when it does
     */
    static boolean underAny(String location, List<String> roots) {
        for (String segment : location.split("/")) {
            if (".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        for (String root : roots) {
            String prefix = root.endsWith("/") ? root : root + "/";
            if (location.equals(root) || location.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
