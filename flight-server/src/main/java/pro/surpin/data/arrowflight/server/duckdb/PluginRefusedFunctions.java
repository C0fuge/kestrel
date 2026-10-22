package pro.surpin.data.arrowflight.server.duckdb;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Functions plugins asked the server to refuse in client queries, on top of the server's own
 * list. Process-wide and add-only: a name once refused stays refused.
 */
public final class PluginRefusedFunctions {

    private static final Set<String> NAMES = ConcurrentHashMap.newKeySet();

    /** Not instantiable. */
    private PluginRefusedFunctions() {
    }

    /**
     * Refuses more functions.
     *
     * @param names function names, any case
     */
    public static void addAll(Set<String> names) {
        for (String name : names) {
            NAMES.add(name.toLowerCase(Locale.ROOT));
        }
    }

    /**
     * Tells whether a plugin refused a function.
     *
     * @param name function name, lower case
     * @return true when refused
     */
    public static boolean contains(String name) {
        return !NAMES.isEmpty() && NAMES.contains(name);
    }
}
