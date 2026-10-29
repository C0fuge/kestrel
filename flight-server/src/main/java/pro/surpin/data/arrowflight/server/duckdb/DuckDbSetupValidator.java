package pro.surpin.data.arrowflight.server.duckdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import pro.surpin.data.arrowflight.spi.config.SecretRedactor;
import pro.surpin.data.arrowflight.spi.duckdb.DuckDbSetup;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

/**
 * Checks what stores and catalogs ask of DuckDB and writes the SQL for it, so that no plugin can
 * weaken the lockdown: extensions resolve only to files the server was configured with; settings
 * are allowlisted with a rule for their values; secrets are temporary, of a known type, with
 * allowlisted options and a scope below the contributor's own roots; refused functions can only
 * be added; roots must be absolute, normalised and of a loaded store's scheme.
 *
 * <p>Order of the statements: the data directory's store, then the other stores by root, then
 * the catalogs; per contributor its extensions, then its settings, then its secrets, each in the
 * order asked. The server's prelude and HDFS block run before them, the lockdown after them.
 */
public final class DuckDbSetupValidator {

    /** Secret types a plugin may create. */
    private static final Set<String> SECRET_TYPES = Set.of("s3", "gcs", "r2", "azure");
    /** The schemes DuckDB's S3 file system reads, which name the same objects; in order. */
    private static final List<String> S3_SCHEMES = List.of("s3", "s3a", "s3n");
    /** Options of the object-store secret types; true marks a boolean option. */
    private static final Map<String, Boolean> OBJECT_STORE_OPTIONS = Map.ofEntries(
            Map.entry("KEY_ID", false), Map.entry("SECRET", false),
            Map.entry("SESSION_TOKEN", false), Map.entry("REGION", false),
            Map.entry("ENDPOINT", false), Map.entry("URL_STYLE", false),
            Map.entry("USE_SSL", true), Map.entry("URL_COMPATIBILITY_MODE", true),
            Map.entry("ACCOUNT_ID", false), Map.entry("KMS_KEY_ID", false),
            Map.entry("REQUESTER_PAYS", true));
    /** Options of Azure secrets. */
    private static final Map<String, Boolean> AZURE_OPTIONS = Map.of(
            "CONNECTION_STRING", false, "ACCOUNT_NAME", false, "TENANT_ID", false,
            "CLIENT_ID", false, "CLIENT_SECRET", false);
    /** Settings that would weaken the lockdown or change the server's own resources. */
    private static final Set<String> REFUSED_SETTINGS = Set.of(
            "enable_external_access", "lock_configuration", "allowed_directories", "allowed_paths",
            "extension_directory", "custom_extension_repository", "secret_directory",
            "temp_directory", "max_temp_directory_size", "threads", "memory_limit",
            "preserve_insertion_order", "streaming_buffer_size",
            "enable_logging", "disable_logging", "enable_profiling", "profiling_output");
    /** Prefixes of settings refused for the same reason. */
    private static final List<String> REFUSED_SETTING_PREFIXES = List.of(
            "allow_", "autoinstall", "autoload", "logging", "enable_logging");
    /** An extension, setting, secret or function name. */
    private static final Pattern IDENTIFIER = Pattern.compile("[a-z_][a-z0-9_]{0,63}");
    /** A secret option name. */
    private static final Pattern OPTION = Pattern.compile("[A-Z][A-Z0-9_]{0,63}");
    /** A URI scheme followed by "://". */
    private static final Pattern SCHEME = Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*)://(.*)$");

    /** What a setting's value may be. */
    private enum Rule {
        /** Only the empty string. */
        EMPTY_ONLY,
        /** Only true. */
        TRUE_ONLY,
        /** Only false. */
        FALSE_ONLY,
        /** Any boolean. */
        BOOLEAN,
        /** A readable file. */
        EXISTING_FILE,
        /** A number of at least zero. */
        NON_NEGATIVE,
        /** Any value (hdfs_*). */
        ANY
    }

    /** Settings a plugin may make, with the rule for their values. */
    private static final Map<String, Rule> SETTINGS = Map.ofEntries(
            Map.entry("s3_access_key_id", Rule.EMPTY_ONLY),
            Map.entry("s3_secret_access_key", Rule.EMPTY_ONLY),
            Map.entry("s3_session_token", Rule.EMPTY_ONLY),
            Map.entry("s3_endpoint", Rule.EMPTY_ONLY),
            Map.entry("s3_region", Rule.EMPTY_ONLY),
            Map.entry("s3_url_compatibility_mode", Rule.TRUE_ONLY),
            Map.entry("enable_http_logging", Rule.FALSE_ONLY),
            Map.entry("enable_server_cert_verification", Rule.BOOLEAN),
            Map.entry("ca_cert_file", Rule.EXISTING_FILE),
            Map.entry("parquet_metadata_cache", Rule.BOOLEAN),
            Map.entry("enable_http_metadata_cache", Rule.BOOLEAN),
            Map.entry("http_timeout", Rule.NON_NEGATIVE),
            Map.entry("http_retries", Rule.NON_NEGATIVE),
            Map.entry("http_retry_wait_ms", Rule.NON_NEGATIVE),
            Map.entry("http_retry_backoff", Rule.NON_NEGATIVE),
            Map.entry("httpfs_connection_caching", Rule.BOOLEAN));

    private final UnaryOperator<String> extensionResolver;
    private final SecretRedactor redactor;

    /**
     * One store or catalog and what it asks for.
     *
     * @param contributor provider name, for errors
     * @param schemes URI schemes the contributor's roots may have; "file" for plain paths
     * @param roots readable roots as DuckDB sees them
     * @param body calls the contributor's {@code contributeDuckDb}
     */
    public record Contribution(String contributor, Set<String> schemes, List<String> roots,
            Consumer<DuckDbSetup> body) {

        /**
         * Copies the collections.
         *
         * @param contributor provider name
         * @param schemes allowed schemes
         * @param roots readable roots
         * @param body contribution
         */
        public Contribution {
            Objects.requireNonNull(contributor, "contributor");
            schemes = Set.copyOf(schemes);
            roots = List.copyOf(roots);
            Objects.requireNonNull(body, "body");
        }

        /**
         * Describes a store whose roots may have the schemes the store itself names them with.
         *
         * @param contributor provider name
         * @param store store
         * @return contribution
         */
        public static Contribution ofStore(String contributor,
                pro.surpin.data.arrowflight.spi.storage.DataStore store) {
            List<String> roots = store.duckDbRoots();
            Set<String> schemes = new LinkedHashSet<>();
            for (String root : roots) {
                java.util.regex.Matcher m = SCHEME.matcher(root == null ? "" : root);
                schemes.add(m.matches() ? m.group(1).toLowerCase(Locale.ROOT) : "file");
            }
            return new Contribution(contributor, schemes, roots, store::contributeDuckDb);
        }
    }

    /**
     * Creates a validator.
     *
     * @param extensionResolver resolves an extension name to its file; throws
     *                          IllegalArgumentException when it is not configured
     */
    public DuckDbSetupValidator(UnaryOperator<String> extensionResolver) {
        this(extensionResolver, new SecretRedactor());
    }

    /**
     * Creates a validator that registers the sensitive secret values in a given redactor.
     *
     * @param extensionResolver resolves an extension name to its file
     * @param redactor receives the sensitive values; the plan carries it
     */
    public DuckDbSetupValidator(UnaryOperator<String> extensionResolver, SecretRedactor redactor) {
        this.extensionResolver = Objects.requireNonNull(extensionResolver, "extensionResolver");
        this.redactor = Objects.requireNonNull(redactor, "redactor");
    }

    /**
     * Checks every contribution and writes the plan.
     *
     * @param dataDirectory the data directory's store
     * @param otherStores other stores, written after it sorted by first root
     * @param catalogs catalogs, written last
     * @return setup plan
     * @throws IllegalArgumentException naming the contributor and the refused item
     */
    public DuckDbSetupPlan plan(Contribution dataDirectory, List<Contribution> otherStores,
            List<Contribution> catalogs) {
        List<Contribution> ordered = new ArrayList<>();
        ordered.add(dataDirectory);
        List<Contribution> others = new ArrayList<>(otherStores);
        others.sort(Comparator.comparing(c -> c.roots().isEmpty() ? "" : c.roots().get(0)));
        ordered.addAll(others);
        ordered.addAll(catalogs);

        // Catalog tables written by other engines may name S3 objects as s3a:// or s3n://.
        boolean s3Catalog = catalogs.stream().flatMap(c -> c.roots().stream())
                .anyMatch(r -> S3_SCHEMES.contains(Recorder.schemeOf(r)));
        Writer writer = new Writer();
        for (Contribution contribution : ordered) {
            List<String> roots = new ArrayList<>();
            for (String root : contribution.roots()) {
                roots.add(checkRoot(root, contribution.schemes(), contribution.contributor()));
            }
            writer.roots.addAll(roots);
            if (catalogs.contains(contribution)) {
                writer.roots.addAll(schemeTwins(roots));
            }
            Recorder recorder = new Recorder(contribution.contributor(), roots, s3Catalog);
            contribution.body().accept(recorder);
            writer.write(recorder);
        }
        return new DuckDbSetupPlan(writer.statements, new ArrayList<>(writer.roots),
                writer.refused, redactor, new ArrayList<>(writer.extensions.keySet()),
                writer.secrets, writer.refusingStatements);
    }

    /**
     * Names catalog roots a second time in the other forms of their scheme. Table formats
     * written by other engines name files that way: Spark and Iceberg's HadoopFileIO write
     * {@code file:/wh/t/metadata/snap-1.avro} for a local warehouse and {@code s3a://b/wh/...}
     * for one written through S3A. DuckDB's readers open those names as written and its
     * allowed_directories check compares them literally (after collapsing repeated slashes), so
     * a plain {@code /wh/} or {@code s3://b/wh/} root refuses them. A twin names the same
     * directory; nothing else becomes readable. Only catalog roots get them: the server names
     * the data directory's files itself, as plain paths and {@code s3://} URIs.
     *
     * @param roots checked roots of one catalog
     * @return {@code file:} twins of the plain absolute paths and the other S3 scheme forms of
     *         the S3 roots
     */
    static List<String> schemeTwins(List<String> roots) {
        List<String> twins = new ArrayList<>();
        for (String root : roots) {
            if (root.startsWith("/")) {
                twins.add("file:" + root);
            } else {
                twins.addAll(s3Twins(root));
            }
        }
        return twins;
    }

    /**
     * Returns the other S3 scheme forms ({@code s3}, {@code s3a}, {@code s3n}) of an S3 URI.
     *
     * @param uri URI
     * @return the same location under the other two schemes; empty for a non-S3 URI
     */
    static List<String> s3Twins(String uri) {
        String scheme = Recorder.schemeOf(uri);
        if (!S3_SCHEMES.contains(scheme)) {
            return List.of();
        }
        String rest = uri.substring(scheme.length());
        List<String> twins = new ArrayList<>();
        for (String other : S3_SCHEMES) {
            if (!other.equals(scheme)) {
                twins.add(other + rest);
            }
        }
        return twins;
    }

    /**
     * Checks a readable root.
     *
     * @param root root as the contributor names it
     * @param schemes schemes the contributor may use
     * @param contributor contributor, for errors
     * @return the root
     * @throws IllegalArgumentException when the root is refused
     */
    static String checkRoot(String root, Set<String> schemes, String contributor) {
        String problem = rootProblem(root, schemes);
        if (problem != null) {
            throw new IllegalArgumentException("Readable root " + quote(root) + " of "
                    + contributor + " is refused: " + problem);
        }
        return root;
    }

    /**
     * Describes what is wrong with a root.
     *
     * @param root root
     * @param schemes allowed schemes
     * @return problem, or null when the root is fine
     */
    static String rootProblem(String root, Set<String> schemes) {
        if (root == null || root.isBlank() || !root.equals(root.strip())) {
            return "empty or padded";
        }
        String scheme;
        String path;
        java.util.regex.Matcher m = SCHEME.matcher(root);
        if (m.matches()) {
            scheme = m.group(1).toLowerCase(Locale.ROOT);
            path = m.group(2);
            int slash = path.indexOf('/');
            if (path.isEmpty() || slash == 0) {
                return "a bare scheme";
            }
            path = slash < 0 ? "" : path.substring(slash);
        } else if (root.startsWith("/")) {
            scheme = "file";
            path = root;
            if ("/".equals(root)) {
                return "the whole file system";
            }
        } else {
            return "not an absolute URI or path";
        }
        if (!schemes.contains(scheme)) {
            return "scheme " + scheme + " is not one of the store's " + schemes;
        }
        for (char c : root.toCharArray()) {
            if (c == '?' || c == '#' || c == '*' || c < 0x20 || c == 0x7f) {
                return "it has ?, #, * or a control character";
            }
        }
        String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        if (trimmed.contains("//")) {
            return "not normalised (//)";
        }
        for (String segment : trimmed.split("/")) {
            if (".".equals(segment) || "..".equals(segment)) {
                return "not normalised (. or ..)";
            }
        }
        return null;
    }

    /**
     * Checks a secret and writes its statement.
     *
     * @param contributor provider name, for errors
     * @param verb statement start, for example {@code CREATE TEMPORARY SECRET }
     * @param name secret name, already checked
     * @param type secret type
     * @param scope URI prefix the secret applies to
     * @param roots contributor's roots the scope must lie below
     * @param options secret options in order
     * @param s3Twins whether S3 secrets are also scoped to the other S3 scheme forms
     * @param sensitive receives the sensitive values
     * @return statement
     * @throws IllegalArgumentException when the type, an option or the scope is refused
     */
    @SuppressWarnings("checkstyle:ParameterNumber")
    static String secretStatement(String contributor, String verb, String name, String type,
            String scope, List<String> roots, Map<String, SecretValue> options, boolean s3Twins,
            List<String> sensitive) {
        String kind = type == null ? "" : type.toLowerCase(Locale.ROOT);
        if (!SECRET_TYPES.contains(kind)) {
            throw refusal(contributor, "secret type " + quote(type) + ", allowed: " + SECRET_TYPES);
        }
        String problem = rootProblem(scope, Set.of(Recorder.schemeOf(scope)));
        if (problem != null || !underRoots(scope, roots)) {
            throw refusal(contributor, "secret " + name + " scope " + quote(scope)
                    + " is not below the contributor's roots " + roots);
        }
        Map<String, Boolean> allowed = "azure".equals(kind) ? AZURE_OPTIONS : OBJECT_STORE_OPTIONS;
        StringBuilder sql = new StringBuilder(verb).append(name).append(" (TYPE ").append(kind);
        for (Map.Entry<String, SecretValue> option : options.entrySet()) {
            String key = option.getKey();
            SecretValue value = option.getValue();
            if (key == null || !OPTION.matcher(key).matches() || !allowed.containsKey(key)) {
                throw refusal(contributor, "secret " + name + " option " + quote(key)
                        + ", allowed: " + allowed.keySet());
            }
            Objects.requireNonNull(value, "secret option value");
            sql.append(", ").append(key).append(' ');
            if (Boolean.TRUE.equals(allowed.get(key))) {
                if (!"true".equals(value.value()) && !"false".equals(value.value())) {
                    throw refusal(contributor, "secret " + name + " option " + key
                            + " must be true or false");
                }
                sql.append(value.value());
            } else {
                sql.append(literal(value.value()));
            }
            if (value.sensitive()) {
                sensitive.add(value.value());
            }
        }
        List<String> twins = s3Twins && "s3".equals(kind) ? s3Twins(scope) : List.of();
        if (twins.isEmpty()) {
            sql.append(", SCOPE ").append(literal(scope)).append(')');
        } else {
            // A catalog may name these objects as s3a:// or s3n://: the same credentials.
            sql.append(", SCOPE [").append(literal(scope));
            for (String twin : twins) {
                sql.append(", ").append(literal(twin));
            }
            sql.append("])");
        }
        return sql.toString();
    }

    /**
     * Builds the error for a refused item.
     *
     * @param contributor provider name
     * @param what refused item
     * @return exception to throw
     */
    private static IllegalArgumentException refusal(String contributor, String what) {
        return new IllegalArgumentException("DuckDB setup of " + contributor + " refused: " + what);
    }

    /**
     * Tells whether a scope lies below one of some roots.
     *
     * @param scope secret scope
     * @param roots contributor's roots
     * @return true when the scope is a root or below one
     */
    static boolean underRoots(String scope, List<String> roots) {
        for (String root : roots) {
            String prefix = root.endsWith("/") ? root : root + "/";
            if (scope.equals(root) || scope.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Quotes a value as a DuckDB string literal.
     *
     * @param value value
     * @return literal
     */
    static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Quotes a value for an error message.
     *
     * @param value value or null
     * @return quoted value
     */
    private static String quote(String value) {
        return value == null ? "null" : "'" + value + "'";
    }

    /**
     * Collects what one contributor asks for and checks each item as it is asked.
     */
    private static final class Recorder implements DuckDbSetup {
        private final String contributor;
        private final List<String> roots;
        private final List<String> extensions = new ArrayList<>();
        /** Extensions whose added functions are refused in client queries. */
        private final Set<String> refusingExtensions = new LinkedHashSet<>();
        /** Setting name to its value as written. */
        private final Map<String, String> settings = new LinkedHashMap<>();
        private final List<String[]> secrets = new ArrayList<>();
        private final List<String> sensitive = new ArrayList<>();
        private final Set<String> refused = new LinkedHashSet<>();
        /** Whether S3 secrets are also scoped to the s3a:// and s3n:// forms of their scope. */
        private final boolean s3Twins;

        /**
         * Creates a recorder.
         *
         * @param contributor contributor, for errors
         * @param roots contributor's checked roots
         */
        Recorder(String contributor, List<String> roots, boolean s3Twins) {
            this.contributor = contributor;
            this.roots = roots;
            this.s3Twins = s3Twins;
        }

        @Override
        public void loadExtension(String name) {
            requireIdentifier(name, "extension");
            if (!extensions.contains(name)) {
                extensions.add(name);
            }
        }

        @Override
        public void loadExtensionRefusingFunctions(String name) {
            loadExtension(name);
            refusingExtensions.add(name);
        }

        @Override
        public void set(String setting, String value) {
            Rule rule = rule(setting);
            Objects.requireNonNull(value, "value");
            boolean ok = switch (rule) {
                case EMPTY_ONLY -> value.isEmpty();
                case EXISTING_FILE -> !value.isEmpty() && Files.isRegularFile(Path.of(value))
                        && Files.isReadable(Path.of(value));
                case ANY -> true;
                default -> false;
            };
            if (!ok) {
                throw refusal("setting " + setting + " = " + quote(value) + " (" + rule + ")");
            }
            put(setting, literal(value));
        }

        @Override
        public void set(String setting, boolean value) {
            Rule rule = rule(setting);
            boolean ok = switch (rule) {
                case TRUE_ONLY -> value;
                case FALSE_ONLY -> !value;
                case BOOLEAN, ANY -> true;
                default -> false;
            };
            if (!ok) {
                throw refusal("setting " + setting + " = " + value + " (" + rule + ")");
            }
            put(setting, Boolean.toString(value));
        }

        @Override
        public void set(String setting, long value) {
            Rule rule = rule(setting);
            boolean ok = rule == Rule.ANY || rule == Rule.NON_NEGATIVE && value >= 0;
            if (!ok) {
                throw refusal("setting " + setting + " = " + value + " (" + rule + ")");
            }
            put(setting, Long.toString(value));
        }

        @Override
        public void createSecret(String name, String type, String scope,
                Map<String, SecretValue> options) {
            requireIdentifier(name, "secret name");
            String sql = secretStatement(contributor, "CREATE TEMPORARY SECRET ", name, type, scope,
                    roots, options, s3Twins, sensitive);
            secrets.add(new String[] {name, scope, sql});
        }

        @Override
        public void refuseFunctions(String... names) {
            for (String name : names) {
                String lower = name == null ? null : name.toLowerCase(Locale.ROOT);
                requireIdentifier(lower, "function");
                refused.add(lower);
            }
        }

        /**
         * Finds the rule of a setting.
         *
         * @param setting setting name
         * @return rule
         * @throws IllegalArgumentException when the setting is refused
         */
        private Rule rule(String setting) {
            requireIdentifier(setting, "setting");
            if (REFUSED_SETTINGS.contains(setting)
                    || REFUSED_SETTING_PREFIXES.stream().anyMatch(setting::startsWith)) {
                throw refusal("setting " + setting + ": only the server sets it");
            }
            Rule rule = SETTINGS.get(setting);
            if (rule == null && setting.startsWith("hdfs_")) {
                rule = Rule.ANY;
            }
            if (rule == null) {
                throw refusal("setting " + setting + " is not on the allowlist");
            }
            return rule;
        }

        /**
         * Records a setting; the same setting twice must have the same value.
         *
         * @param setting setting name
         * @param written value as written
         */
        private void put(String setting, String written) {
            String previous = settings.putIfAbsent(setting, written);
            if (previous != null && !previous.equals(written)) {
                throw refusal("setting " + setting + " set twice with different values");
            }
        }

        /**
         * Checks a name.
         *
         * @param name name
         * @param what what it names
         */
        private void requireIdentifier(String name, String what) {
            if (name == null || !IDENTIFIER.matcher(name).matches()) {
                throw refusal(what + " name " + quote(name));
            }
        }

        /**
         * Builds the error for a refused item.
         *
         * @param what refused item
         * @return exception to throw
         */
        private IllegalArgumentException refusal(String what) {
            return new IllegalArgumentException("DuckDB setup of " + contributor + " refused: "
                    + what);
        }

        /**
         * Returns the scheme of a URI, "file" for a path.
         *
         * @param uri URI or path
         * @return lower case scheme
         */
        private static String schemeOf(String uri) {
            java.util.regex.Matcher m = SCHEME.matcher(uri == null ? "" : uri);
            return m.matches() ? m.group(1).toLowerCase(Locale.ROOT) : "file";
        }
    }

    /**
     * Writes the statements of all contributors and merges what they declare.
     */
    private final class Writer {
        private final List<String> statements = new ArrayList<>();
        private final Set<String> roots = new LinkedHashSet<>();
        /** Extension name to its file. */
        private final Map<String, String> extensions = new LinkedHashMap<>();
        /** Setting name to the value written. */
        private final Map<String, String> settings = new LinkedHashMap<>();
        private final List<DuckDbSetupPlan.DeclaredSecret> secrets = new ArrayList<>();
        private final Set<String> refused = new LinkedHashSet<>();
        /** Extension name to the index of its LOAD statement. */
        private final Map<String, Integer> loadStatements = new LinkedHashMap<>();
        /** Statements whose added functions are refused. */
        private final Set<Integer> refusingStatements = new LinkedHashSet<>();

        /**
         * Writes one contributor's items.
         *
         * @param recorder what the contributor asked for
         */
        void write(Recorder recorder) {
            for (String name : recorder.extensions) {
                if (!extensions.containsKey(name)) {
                    String file = extensionResolver.apply(name);
                    extensions.put(name, file);
                    loadStatements.put(name, statements.size());
                    statements.add("LOAD " + literal(file));
                }
                if (recorder.refusingExtensions.contains(name)) {
                    // Also when another contributor loaded it first: its LOAD adds the functions.
                    refusingStatements.add(loadStatements.get(name));
                }
            }
            for (Map.Entry<String, String> setting : recorder.settings.entrySet()) {
                String previous = settings.putIfAbsent(setting.getKey(), setting.getValue());
                if (previous == null) {
                    statements.add("SET " + setting.getKey() + " = " + setting.getValue());
                } else if (!previous.equals(setting.getValue())) {
                    throw new IllegalArgumentException("DuckDB setup of " + recorder.contributor
                            + " refused: setting " + setting.getKey()
                            + " differs from what another store set");
                }
            }
            for (String[] secret : recorder.secrets) {
                if (secrets.stream().anyMatch(s -> s.name().equals(secret[0]))) {
                    throw new IllegalArgumentException("DuckDB setup of " + recorder.contributor
                            + " refused: secret " + secret[0] + " exists already");
                }
                secrets.add(new DuckDbSetupPlan.DeclaredSecret(secret[0], secret[1]));
                statements.add(secret[2]);
            }
            recorder.sensitive.forEach(DuckDbSetupValidator.this.redactor::add);
            refused.addAll(recorder.refused);
        }
    }
}
