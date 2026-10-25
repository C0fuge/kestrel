package pro.surpin.data.arrowflight.plugin.s3;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.spi.config.PluginConfig;

/**
 * The further S3 stores of the s3 namespace, for catalog roots outside the data directory:
 * {@code arrowflight.s3.stores=<name>,...} and per name {@code arrowflight.s3.<name>.root} plus
 * the usual keys below {@code arrowflight.s3.<name>.} ({@code FLIGHT_S3_<NAME>_ACCESS_KEY}, ...).
 * The endpoint, region, URL style and credentials are the store's own; the settings DuckDB holds
 * once per connection (CA file, TLS check, metadata cache) and the listing settings fall back to
 * the main store's.
 */
final class S3NamedStores {

    /** Key that names the further stores. */
    static final String STORES_KEY = "stores";
    /** Store names: lower case, so that the environment variable names stay unambiguous. */
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9]{0,15}");
    /** Keys a named store takes from the main store when it does not set them. */
    private static final Set<String> INHERITED = Set.of("caFile", "verifyTls", "metadataCache",
            "listingTtlSeconds", "maxConnections");

    private S3NamedStores() {
    }

    /**
     * One further store.
     *
     * @param name store name
     * @param root the bucket prefix it serves
     * @param config its configuration view
     */
    record Store(String name, S3Uri root, PluginConfig config) {
    }

    /**
     * Reads the further stores.
     *
     * @param config configuration of the s3 namespace
     * @return stores in configured order
     * @throws IllegalArgumentException when a name is invalid or repeated, or a store has no root
     */
    static List<Store> from(PluginConfig config) {
        Set<String> names = new LinkedHashSet<>();
        for (String part : config.get(STORES_KEY, "").split(",", -1)) {
            String name = part.trim();
            if (name.isEmpty()) {
                continue;
            }
            if (!NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("s3.stores: invalid store name " + name
                        + " (lower case letters and digits, starting with a letter, at most 16)");
            }
            if (!names.add(name)) {
                throw new IllegalArgumentException("s3.stores names " + name + " twice");
            }
        }
        List<Store> stores = new ArrayList<>();
        for (String name : names) {
            String root = config.get(name + ".root").orElseThrow(() -> new IllegalArgumentException(
                    "s3 store " + name + " needs s3." + name + ".root (s3://bucket/prefix)"));
            stores.add(new Store(name, S3Uri.parse(root), new View(config, name)));
        }
        for (int i = 0; i < stores.size(); i++) {
            for (int j = i + 1; j < stores.size(); j++) {
                if (stores.get(i).root().equals(stores.get(j).root())) {
                    throw new IllegalArgumentException("s3 stores " + stores.get(i).name() + " and "
                            + stores.get(j).name() + " have the same root " + stores.get(i).root().uri());
                }
            }
        }
        return stores;
    }

    /**
     * Finds the store with the longest root that holds a location.
     *
     * @param stores further stores
     * @param location S3 location
     * @return the store, empty when none holds it
     */
    static Optional<Store> holding(List<Store> stores, S3Uri location) {
        Store best = null;
        for (Store store : stores) {
            boolean holds = store.root().bucket().equals(location.bucket())
                    && location.prefix().startsWith(store.root().prefix());
            if (holds && (best == null
                    || store.root().prefix().length() > best.root().prefix().length())) {
                best = store;
            }
        }
        return Optional.ofNullable(best);
    }

    /**
     * The configuration of one named store, read below {@code s3.<name>.}.
     */
    private static final class View implements PluginConfig {
        private final PluginConfig main;
        private final String name;

        /**
         * Creates the view.
         *
         * @param main configuration of the s3 namespace
         * @param name store name
         */
        View(PluginConfig main, String name) {
            this.main = Objects.requireNonNull(main, "main");
            this.name = Objects.requireNonNull(name, "name");
        }

        @Override
        public String namespace() {
            return main.namespace() + "." + name;
        }

        @Override
        public Optional<String> get(String key) {
            Optional<String> own = main.get(name + "." + key);
            if (own.isPresent() || !INHERITED.contains(key)) {
                return own;
            }
            return main.get(key);
        }

        @Override
        public Optional<String> secret(String key) {
            return main.secret(name + "." + key);
        }

        @Override
        public String toString() {
            return "S3NamedStores.View[" + name + "]";
        }
    }
}
