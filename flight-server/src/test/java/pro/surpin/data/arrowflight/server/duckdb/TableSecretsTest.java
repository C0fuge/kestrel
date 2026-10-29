package pro.surpin.data.arrowflight.server.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import pro.surpin.data.arrowflight.spi.catalog.DeleteFile;
import pro.surpin.data.arrowflight.spi.catalog.ReadSpec;
import pro.surpin.data.arrowflight.spi.catalog.StorageCredential;
import pro.surpin.data.arrowflight.spi.catalog.TableFile;
import pro.surpin.data.arrowflight.spi.catalog.TableName;
import pro.surpin.data.arrowflight.spi.catalog.TableSnapshot;
import pro.surpin.data.arrowflight.spi.duckdb.SecretValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Tag("unit")
class TableSecretsTest {

    private static final Instant NOW = Instant.parse("2026-10-29T10:00:00Z");
    private static final List<String> ROOTS = List.of("s3://lake/wh");

    private final TableSecrets secrets = new TableSecrets(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void scopeIsTheDirectoryOfTheTablesFilesBelowThePrefix() {
        List<String> locations = List.of("s3://lake/wh/ns/t/data/a/1.parquet",
                "s3://lake/wh/ns/t/data/b/2.parquet", "s3://lake/wh/ns/t/metadata/v3.json",
                "s3://lake/elsewhere/3.parquet");
        assertEquals("s3://lake/wh/ns/t/", TableSecrets.scope("s3://lake/wh/ns/t", locations));
        // A wide prefix (the whole warehouse) still gives only the table's directory, unless the
        // table has files elsewhere below it.
        assertEquals("s3://lake/wh/ns/t/", TableSecrets.scope("s3://lake/wh", locations));
        assertEquals("s3://lake/", TableSecrets.scope("s3://lake", locations));
        assertEquals("s3://lake/wh/ns/t/data/a/",
                TableSecrets.scope("s3://lake/wh/ns/t/data/a", locations));
        assertEquals("s3://lake/elsewhere/", TableSecrets.scope("s3://lake/elsewhere", locations));
        assertNull(TableSecrets.scope("s3://lake/wh/ns/u", locations));
        // A prefix is a directory: s3://lake/wh/ns/t does not cover s3://lake/wh/ns/t2.
        assertNull(TableSecrets.scope("s3://lake/wh/ns/t",
                List.of("s3://lake/wh/ns/t2/data/1.parquet")));
        // Common characters past the last common slash do not count.
        assertEquals("s3://lake/wh/ns/t/data/", TableSecrets.scope("s3://lake/wh/ns/t", List.of(
                "s3://lake/wh/ns/t/data/part-1.parquet", "s3://lake/wh/ns/t/data/part-2.parquet")));
    }

    @Test
    void locationsCoverDeleteFilesAndIcebergMetadataNormalised() {
        TableFile file = new TableFile("s3a://lake/wh/t/data/1.parquet", 1, 0, null, Map.of(), -1,
                List.of(new DeleteFile("s3a://lake/wh/t/data/d.parquet", 1,
                        DeleteFile.Kind.POSITION, null)));
        TableSnapshot snapshot = new TableSnapshot(new TableName("ns", "t"), "1", "f", null, null,
                List.of(file), new ReadSpec.IcebergMetadata("s3n://lake/wh/t/metadata/v1.json"));
        assertEquals(List.of("s3://lake/wh/t/data/1.parquet", "s3://lake/wh/t/data/d.parquet",
                "s3://lake/wh/t/metadata/v1.json"), TableSecrets.locations(snapshot));
    }

    @Test
    void registersOneScopedSecretPerCredential() throws SQLException {
        secrets.register("rest", ROOTS, snapshot("t", credential("s3a://lake/wh/ns/t", "tok1",
                NOW.plusSeconds(3600))));
        TableSecrets.State state = secrets.current();
        assertEquals(1, state.version());
        assertEquals(1, state.statements().size());
        String sql = state.statements().values().iterator().next();
        String name = state.statements().keySet().iterator().next();
        assertTrue(name.startsWith(TableSecrets.NAME_PREFIX), name);
        assertEquals("CREATE OR REPLACE TEMPORARY SECRET " + name + " (TYPE s3, KEY_ID 'AKIAV',"
                + " SECRET 'vended-secret', SESSION_TOKEN 'tok1', REGION 'us-east-1',"
                + " SCOPE ['s3://lake/wh/ns/t/data/', 's3a://lake/wh/ns/t/data/',"
                + " 's3n://lake/wh/ns/t/data/'])", sql);

        List<String> run = new ArrayList<>();
        Statement statement = recording(run);
        Map<String, String> applied = new HashMap<>();
        assertEquals(1, secrets.apply(statement, applied, TableSecretsTest::fail));
        assertEquals(List.of(sql), run);
        // Nothing changed: nothing runs again.
        secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/t", "tok1",
                NOW.plusSeconds(3600))));
        assertEquals(1, secrets.current().version());
        assertEquals(1, secrets.apply(statement, applied, TableSecretsTest::fail));
        assertEquals(1, run.size());
    }

    @Test
    void refreshedCredentialsReplaceAndVanishedOnesAreDropped() throws SQLException {
        List<String> run = new ArrayList<>();
        Statement statement = recording(run);
        Map<String, String> applied = new HashMap<>();
        secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/t", "tok1", null)));
        secrets.register("rest", ROOTS, snapshot("u", credential("s3://lake/wh/ns/u", "tokU", null)));
        secrets.apply(statement, applied, TableSecretsTest::fail);
        assertEquals(2, run.size());
        assertEquals(2, applied.size());

        secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/t", "tok2", null)));
        run.clear();
        secrets.apply(statement, applied, TableSecretsTest::fail);
        assertEquals(1, run.size());
        assertTrue(run.get(0).startsWith("CREATE OR REPLACE TEMPORARY SECRET"));
        assertTrue(run.get(0).contains("'tok2'"));

        // The table is now read with the server's own credentials.
        secrets.register("rest", ROOTS, snapshot("t"));
        run.clear();
        long version = secrets.apply(statement, applied, TableSecretsTest::fail);
        assertEquals(1, run.size());
        assertTrue(run.get(0).startsWith("DROP TEMPORARY SECRET IF EXISTS " + TableSecrets.NAME_PREFIX));
        assertEquals(1, applied.size());
        assertEquals(1, secrets.current().statements().size());
        assertEquals(version, secrets.current().version());
        // Another catalog's table of the same name has its own secret.
        secrets.register("other", ROOTS, snapshot("u", credential("s3://lake/wh/ns/u", "tokO", null)));
        assertEquals(2, secrets.current().statements().size());
    }

    @Test
    void refusesScopesOutsideTheCatalogsRootsAndExpiredCredentials() {
        IllegalArgumentException outside = assertThrows(IllegalArgumentException.class,
                () -> secrets.register("rest", ROOTS, snapshot("t",
                        new StorageCredential("s3://lake/other", "s3", options("tok"), null),
                        "s3://lake/other/t/1.parquet")));
        assertTrue(outside.getMessage().contains("not below the contributor's roots"),
                outside.getMessage());
        assertFalse(outside.getMessage().contains("vended-secret"));
        IllegalArgumentException type = assertThrows(IllegalArgumentException.class,
                () -> secrets.register("rest", ROOTS, snapshot("t",
                        new StorageCredential("s3://lake/wh/ns/t", "http", options("tok"), null))));
        assertTrue(type.getMessage().contains("secret type"), type.getMessage());
        Map<String, SecretValue> bad = new LinkedHashMap<>(options("tok"));
        bad.put("PROVIDER", SecretValue.plain("credential_chain"));
        assertThrows(IllegalArgumentException.class, () -> secrets.register("rest", ROOTS,
                snapshot("t", new StorageCredential("s3://lake/wh/ns/t", "s3", bad, null))));
        IllegalStateException expired = assertThrows(IllegalStateException.class,
                () -> secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/t",
                        "tok", NOW))));
        assertTrue(expired.getMessage().contains("expired"), expired.getMessage());
        assertEquals(0, secrets.current().version());
        // A credential for files the snapshot does not have is not installed.
        secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/x", "tok", null)));
        assertEquals(0, secrets.current().statements().size());
    }

    @Test
    void refusedStatementsAreReportedWithoutSecretValuesAndNotRetried() throws SQLException {
        secrets.register("rest", ROOTS, snapshot("t", credential("s3://lake/wh/ns/t", "tok1", null)));
        Statement statement = mock(Statement.class);
        when(statement.execute(anyString())).thenThrow(new SQLException(
                "Parser Error: syntax error at or near \"SECRET 'vended-secret', SESSION_TOKEN 'tok1"));
        List<String> problems = new ArrayList<>();
        Map<String, String> applied = new HashMap<>();
        assertEquals(1, secrets.apply(statement, applied, problems::add));
        assertEquals(1, problems.size());
        String message = problems.get(0);
        assertFalse(message.contains("vended-secret"), message);
        assertFalse(message.contains("tok1"), message);
        assertTrue(message.startsWith("Table secret " + TableSecrets.NAME_PREFIX), message);
        secrets.apply(statement, applied, problems::add);
        assertEquals(1, problems.size());
    }

    private static void fail(String problem) {
        throw new AssertionError(problem);
    }

    private static Statement recording(List<String> run) throws SQLException {
        Statement statement = mock(Statement.class);
        when(statement.execute(anyString())).thenAnswer(call -> {
            run.add(call.getArgument(0));
            return false;
        });
        return statement;
    }

    private static Map<String, SecretValue> options(String token) {
        Map<String, SecretValue> options = new LinkedHashMap<>();
        options.put("KEY_ID", SecretValue.plain("AKIAV"));
        options.put("SECRET", SecretValue.sensitive("vended-secret"));
        options.put("SESSION_TOKEN", SecretValue.sensitive(token));
        options.put("REGION", SecretValue.plain("us-east-1"));
        return options;
    }

    private static StorageCredential credential(String prefix, String token, Instant expiresAt) {
        return new StorageCredential(prefix, "s3", options(token), expiresAt);
    }

    private static TableSnapshot snapshot(String table, StorageCredential credential) {
        return snapshot(table, credential, "s3://lake/wh/ns/" + table + "/data/1.parquet");
    }

    private static TableSnapshot snapshot(String table, StorageCredential credential, String file) {
        return new TableSnapshot(new TableName("ns", table), "1", "f", null, null,
                List.of(new TableFile(file, 1, 0, null, Map.of(), -1, List.of())),
                new ReadSpec.ParquetFiles(false), List.of(credential));
    }

    private static TableSnapshot snapshot(String table) {
        return new TableSnapshot(new TableName("ns", table), "1", "f", null, null,
                List.of(new TableFile("s3://lake/wh/ns/" + table + "/data/1.parquet", 1, 0, null,
                        Map.of(), -1, List.of())), new ReadSpec.ParquetFiles(false));
    }
}
