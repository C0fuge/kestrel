package pro.surpin.data.arrowflight.server.duckdb;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import pro.surpin.data.arrowflight.spi.config.SecretRedactor;

/**
 * What every DuckDB connection runs for its stores and catalogs, as checked and written out by
 * {@link DuckDbSetupValidator}: the statements run after the server's own prelude and the HDFS
 * block, and the roots that become DuckDB's allowed directories.
 *
 * @param statements setup statements in order; they may hold credentials and are never logged
 * @param roots readable roots in order: the data directory's store first
 * @param extraRefusedFunctions functions refused in client queries on top of the server's own
 * @param redactor removes the secret values of the setup from errors
 * @param extensions names of the extensions loaded
 * @param secrets declared secrets, checked against {@code duckdb_secrets()} at startup; null
 *                when the statements were not written by the validator and are not checked
 * @param refusingStatements indexes of the statements (extension loads) whose added functions
 *                           are refused in client queries: {@code duckdb_functions()} is compared
 *                           before and after each of them
 */
public record DuckDbSetupPlan(List<String> statements, List<String> roots,
        Set<String> extraRefusedFunctions, SecretRedactor redactor, List<String> extensions,
        List<DeclaredSecret> secrets, Set<Integer> refusingStatements) {

    /**
     * A secret the setup creates.
     *
     * @param name secret name
     * @param scope scope prefix
     */
    public record DeclaredSecret(String name, String scope) {
    }

    /**
     * Copies the lists.
     *
     * @param statements setup statements
     * @param roots readable roots
     * @param extraRefusedFunctions extra refused functions
     * @param redactor redactor
     * @param extensions loaded extensions
     * @param secrets declared secrets, or null when unknown
     * @param refusingStatements statements whose added functions are refused
     */
    public DuckDbSetupPlan {
        refusingStatements = Set.copyOf(refusingStatements);
        for (int index : refusingStatements) {
            if (index < 0 || index >= statements.size()) {
                throw new IllegalArgumentException("No setup statement " + index);
            }
        }
        statements = List.copyOf(statements);
        roots = List.copyOf(roots);
        extraRefusedFunctions = Set.copyOf(extraRefusedFunctions);
        Objects.requireNonNull(redactor, "redactor");
        extensions = List.copyOf(extensions);
        secrets = secrets == null ? null : List.copyOf(secrets);
    }

    /**
     * A plan without statements whose added functions are refused.
     *
     * @param statements setup statements
     * @param roots readable roots
     * @param extraRefusedFunctions extra refused functions
     * @param redactor redactor
     * @param extensions loaded extensions
     * @param secrets declared secrets, or null when unknown
     */
    public DuckDbSetupPlan(List<String> statements, List<String> roots,
            Set<String> extraRefusedFunctions, SecretRedactor redactor, List<String> extensions,
            List<DeclaredSecret> secrets) {
        this(statements, roots, extraRefusedFunctions, redactor, extensions, secrets, Set.of());
    }

    /**
     * A plan with only readable roots and statements whose secrets are not known.
     *
     * @param roots readable roots
     * @param statements setup statements
     * @return plan
     */
    public static DuckDbSetupPlan of(List<String> roots, List<String> statements) {
        return new DuckDbSetupPlan(statements, roots, Set.of(), new SecretRedactor(), List.of(),
                null);
    }

    @Override
    public String toString() {
        // The statements may hold credentials.
        return "DuckDbSetupPlan[statements=" + statements.size() + ", roots=" + roots
                + ", extensions=" + extensions + ", secrets=" + (secrets == null ? "?" : secrets.size())
                + ", extraRefusedFunctions=" + extraRefusedFunctions
                + ", refusingStatements=" + refusingStatements + "]";
    }
}
