package pro.surpin.data.arrowflight.server.duckdb;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import pro.surpin.data.arrowflight.server.catalog.CatalogLocations;
import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;

/**
 * Temporary DuckDB secrets of single tables, from the storage credentials a catalog returns with
 * a snapshot (SPI 1.7). Each credential becomes one secret scoped to the directory that holds
 * the snapshot's files below the credential's prefix, so it reaches no other table; the scope
 * must also lie below one of the catalog's readable roots. Connections apply the current secrets
 * before they run a query ({@link #apply}); a refreshed credential replaces its secret. Secret
 * values never appear in errors or logs.
 */
public final class TableSecrets {

    /** Prefix of the names of table secrets. */
    static final String NAME_PREFIX = "table_secret_";
    /** A string literal in a DuckDB error, possibly cut short. */
    private static final Pattern LITERAL = Pattern.compile("'(?:[^']|'')*'?");

    private final Clock clock;
    /** Table key to the names of its secrets. */
    private final Map<String, List<String>> namesByTable = new HashMap<>();
    /** Secret name to its statement. */
    private final Map<String, String> statements = new LinkedHashMap<>();
    /** Changes with every added, replaced or dropped secret; guarded by this. */
    private long version;
    /** Statements and drops of the current version, read without the lock. */
    private volatile State state = new State(0, Map.of());

    /**
     * The secrets of one version.
     *
     * @param version version
     * @param statements secret name to its statement
     */
    public record State(long version, Map<String, String> statements) {
    }

    /**
     * Creates an empty registry on the system clock.
     */
    public TableSecrets() {
        this(Clock.systemUTC());
    }

    /**
     * Creates an empty registry.
     *
     * @param clock clock for expiry checks
     */
    public TableSecrets(Clock clock) {
        this.clock = clock;
    }

    /**
     * Registers the credentials of a snapshot, replacing the table's earlier ones.
     *
     * @param catalog catalog name, for the secret names and errors
     * @param roots the catalog's readable roots, normalised
     * @param snapshot snapshot as the catalog returned it
     * @throws IllegalArgumentException when a credential's type, an option or its scope is refused
     * @throws IllegalStateException when a credential has expired
     */
    public void register(String catalog, List<String> roots, TableSnapshot snapshot) {
        String table = catalog + "." + snapshot.name();
        if (snapshot.credentials().isEmpty()) {
            synchronized (this) {
                if (namesByTable.containsKey(table)) {
                    replace(table, Map.of());
                }
            }
            return;
        }
        List<String> locations = locations(snapshot);
        Map<String, String> planned = new LinkedHashMap<>();
        Instant now = clock.instant();
        List<StorageCredential> credentials = snapshot.credentials();
        for (int i = 0; i < credentials.size(); i++) {
            StorageCredential credential = credentials.get(i);
            if (credential.expiresAt() != null && !credential.expiresAt().isAfter(now)) {
                throw new IllegalStateException("Storage credentials of table " + table
                        + " for " + credential.prefix() + " expired at " + credential.expiresAt());
            }
            String scope = scope(CatalogLocations.normalize(credential.prefix()), locations);
            if (scope == null) {
                continue;
            }
            String name = NAME_PREFIX + digest(table) + "_" + i;
            planned.put(name, DuckDbSetupValidator.secretStatement("catalog " + catalog,
                    "CREATE OR REPLACE TEMPORARY SECRET ", name, credential.type(), scope, roots,
                    credential.options(), true, new ArrayList<>()));
        }
        synchronized (this) {
            replace(table, planned);
        }
    }

    /**
     * Returns the current secrets.
     *
     * @return secrets of the current version
     */
    public State current() {
        return state;
    }

    /**
     * Brings a connection from the secrets it has to the current ones. A statement DuckDB
     * refuses is reported and counted as applied, so that one table's secret cannot stop every
     * query of the connection; that table's reads then fail on their own.
     *
     * @param statement statement of the connection
     * @param applied secret name to statement the connection has; updated
     * @param problems receives one message per refused statement; it holds no literal
     * @return version the connection now has
     */
    public long apply(Statement statement, Map<String, String> applied, Consumer<String> problems) {
        State now = state;
        for (Map.Entry<String, String> secret : now.statements().entrySet()) {
            if (!secret.getValue().equals(applied.get(secret.getKey()))) {
                run(statement, secret.getValue(), secret.getKey(), problems);
                applied.put(secret.getKey(), secret.getValue());
            }
        }
        for (String name : new ArrayList<>(applied.keySet())) {
            if (!now.statements().containsKey(name)) {
                run(statement, "DROP TEMPORARY SECRET IF EXISTS " + name, name, problems);
                applied.remove(name);
            }
        }
        return now.version();
    }

    /**
     * Replaces the secrets of one table; the caller holds the lock.
     *
     * @param table table key
     * @param planned secret name to statement
     */
    private void replace(String table, Map<String, String> planned) {
        List<String> previous = namesByTable.getOrDefault(table, List.of());
        boolean changed = false;
        for (String name : previous) {
            if (!planned.containsKey(name)) {
                statements.remove(name);
                changed = true;
            }
        }
        for (Map.Entry<String, String> secret : planned.entrySet()) {
            String old = statements.put(secret.getKey(), secret.getValue());
            changed |= !secret.getValue().equals(old);
        }
        if (planned.isEmpty()) {
            namesByTable.remove(table);
        } else {
            namesByTable.put(table, List.copyOf(planned.keySet()));
        }
        if (changed) {
            version++;
            state = new State(version, Map.copyOf(statements));
        }
    }

    /**
     * Runs one secret statement; a failure is reported naming the secret only.
     *
     * @param statement statement of the connection
     * @param sql secret statement
     * @param name secret name
     * @param problems receives the failure
     */
    private static void run(Statement statement, String sql, String name,
            Consumer<String> problems) {
        try {
            statement.execute(sql);
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : LITERAL.matcher(e.getMessage())
                    .replaceAll("'***'");
            problems.accept("Table secret " + name + " cannot be installed: "
                    + (message.length() > 300 ? message.substring(0, 300) + "..." : message));
        }
    }

    /**
     * Lists every location DuckDB reads for a snapshot, normalised.
     *
     * @param snapshot snapshot
     * @return data, delete and metadata file locations
     */
    static List<String> locations(TableSnapshot snapshot) {
        List<String> locations = new ArrayList<>();
        for (TableFile file : snapshot.files()) {
            locations.add(CatalogLocations.normalize(file.location()));
            for (DeleteFile delete : file.deletes()) {
                locations.add(CatalogLocations.normalize(delete.location()));
            }
        }
        if (snapshot.read() instanceof ReadSpec.IcebergMetadata metadata) {
            locations.add(CatalogLocations.normalize(metadata.metadataLocation()));
        }
        return locations;
    }

    /**
     * Chooses the scope of a credential: the deepest directory holding every location below its
     * prefix.
     *
     * @param prefix credential prefix, normalised
     * @param locations locations of the snapshot
     * @return scope ending in a slash, or null when no location lies below the prefix
     */
    static String scope(String prefix, List<String> locations) {
        String directory = prefix.endsWith("/") ? prefix : prefix + "/";
        String common = null;
        for (String location : locations) {
            if (!location.startsWith(directory)) {
                continue;
            }
            if (common == null) {
                common = location.substring(0, location.lastIndexOf('/') + 1);
            } else {
                int n = 0;
                int max = Math.min(common.length(), location.length());
                while (n < max && common.charAt(n) == location.charAt(n)) {
                    n++;
                }
                common = common.substring(0, common.lastIndexOf('/', n - 1) + 1);
            }
        }
        if (common == null) {
            return null;
        }
        return common.length() < directory.length() ? directory : common;
    }

    /**
     * Hashes a table key into a secret name part.
     *
     * @param table table key
     * @return 16 hex digits
     */
    private static String digest(String table) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(table.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
