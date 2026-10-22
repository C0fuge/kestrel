package pro.surpin.data.arrowflight.plugin.iceberg;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

import pro.surpin.data.arrowflight.spi.storage.DataStore;
import pro.surpin.data.arrowflight.spi.storage.Located;
import pro.surpin.data.arrowflight.spi.storage.ReadableFile;
import pro.surpin.data.arrowflight.spi.storage.Stores;

/**
 * Tables of a warehouse directory in the layout of Iceberg's HadoopCatalog:
 * {@code <warehouse>/<namespace>/<table>/metadata/v<n>.metadata.json}, the current version named
 * by {@code metadata/version-hint.text}. Listed and read through the server's store of the
 * warehouse.
 */
final class HadoopTableSource implements TableSource {

    private static final String METADATA = "metadata";
    private static final String VERSION_HINT = "version-hint.text";
    /** Highest version probed when a table has no version hint. */
    private static final int MAX_PROBED_VERSIONS = 100_000;

    private final DataStore store;
    private final String base;
    private final String warehouse;
    private final StoresFileIO io;

    /**
     * Opens the warehouse.
     *
     * @param stores the server's stores
     * @param warehouse warehouse directory as configured
     * @param io file IO over the stores
     * @throws IOException when no store holds the warehouse
     */
    HadoopTableSource(Stores stores, String warehouse, StoresFileIO io) throws IOException {
        Located located = stores.locate(Locations.normalize(warehouse));
        this.store = located.store();
        this.base = located.relativePath();
        this.warehouse = Locations.normalize(warehouse);
        this.io = io;
    }

    @Override
    public List<String> namespaces() throws IOException {
        if (!store.exists(base)) {
            return List.of();
        }
        return new ArrayList<>(store.directories(base).keySet());
    }

    @Override
    public List<String> tables(String namespace) throws IOException {
        String dir = path(namespace);
        if (!store.exists(dir)) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String table : store.directories(dir).keySet()) {
            if (store.exists(path(namespace, table, METADATA))) {
                result.add(table);
            }
        }
        return result;
    }

    @Override
    public Optional<Loaded> load(String namespace, String table) throws IOException {
        String metadataDir = path(namespace, table, METADATA);
        if (!store.exists(metadataDir)) {
            return Optional.empty();
        }
        int version = currentVersion(metadataDir);
        if (version <= 0) {
            return Optional.empty();
        }
        String file = metadataDir + "/v" + version + ".metadata.json";
        if (!store.exists(file)) {
            String gz = metadataDir + "/v" + version + ".gz.metadata.json";
            if (!store.exists(gz)) {
                throw new IOException("Table " + namespace + "." + table + " names version " + version
                        + " in its version hint, but has no metadata file for it");
            }
            file = gz;
        }
        String location = warehouse + (base.isEmpty() ? "/" + file : file.substring(base.length()));
        TableMetadata metadata = TableMetadataParser.read(io, location);
        return Optional.of(new Loaded(location, metadata));
    }

    /**
     * Finds the current metadata version: the version hint, else the highest v&lt;n&gt; file.
     *
     * @param metadataDir metadata directory below the store's root
     * @return version, 0 when there is none
     * @throws IOException on storage failure
     */
    private int currentVersion(String metadataDir) throws IOException {
        String hint = metadataDir + "/" + VERSION_HINT;
        if (store.exists(hint)) {
            ReadableFile file = store.open(hint);
            byte[] bytes = new byte[(int) Math.min(file.length(), 64)];
            file.readFully(0, bytes, 0, bytes.length);
            String text = new String(bytes, StandardCharsets.UTF_8).strip();
            try {
                int version = Integer.parseInt(text);
                // The hint can lag behind a commit that wrote the next file but not the hint.
                while (version < MAX_PROBED_VERSIONS
                        && store.exists(metadataDir + "/v" + (version + 1) + ".metadata.json")) {
                    version++;
                }
                return version;
            } catch (NumberFormatException e) {
                throw new IOException("Bad version hint in " + hint + ": " + text, e);
            }
        }
        int version = 0;
        while (version < MAX_PROBED_VERSIONS
                && store.exists(metadataDir + "/v" + (version + 1) + ".metadata.json")) {
            version++;
        }
        return version;
    }

    /**
     * Joins path segments below the warehouse.
     *
     * @param segments segments
     * @return path below the store's root
     */
    private String path(String... segments) {
        StringBuilder result = new StringBuilder(base);
        for (String segment : segments) {
            if (result.length() > 0) {
                result.append('/');
            }
            result.append(segment);
        }
        return result.toString();
    }
}
