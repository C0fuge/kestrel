package pro.surpin.data.arrowflight.server.catalog;

import java.util.List;
import java.util.Locale;

/**
 * Names the locations a catalog returns the way the stores and DuckDB name them: a local file as
 * a plain absolute path ({@code file:/x}, {@code file:///x} and {@code /x} alike), an S3 object
 * as {@code s3://} ({@code s3a://} and {@code s3n://} alike), every other URI as written; and
 * tells whether a location lies under a root.
 */
public final class CatalogLocations {

    private CatalogLocations() {
    }

    /**
     * Normalises a location.
     *
     * @param location location, URI or path
     * @return plain path for a local file, {@code s3://} for an S3 object, the location
     *         otherwise; without trailing slashes
     */
    public static String normalize(String location) {
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
     * Tells whether a location lies under one of some roots; a location with a "." or ".."
     * segment never does.
     *
     * @param location location (normalised here)
     * @param roots roots (normalised here)
     * @return true when it does
     */
    public static boolean underAny(String location, List<String> roots) {
        String normalized = normalize(location);
        for (String segment : normalized.split("/")) {
            if (".".equals(segment) || "..".equals(segment)) {
                return false;
            }
        }
        for (String root : roots) {
            String r = normalize(root);
            String prefix = r.endsWith("/") ? r : r + "/";
            if (normalized.equals(r) || normalized.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
