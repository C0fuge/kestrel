package pro.surpin.data.arrowflight.server.duckdb;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests that the error DuckDB reports is read out of the JDBC driver's wrapper. */
@Tag("unit")
class DuckDbErrorsTest {

    /**
     * Regression (HDFS on a real cluster, 2026-11-02): a statement that fails while it runs
     * reports DuckDB's pending-result wrapper on its first line; the error itself follows.
     */
    @Test
    void readsTheErrorOfAStatementThatFailedWhileItRan() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement s = conn.createStatement()) {
            SQLException e = assertThrows(SQLException.class, () -> s.executeQuery(
                    "SELECT * FROM read_parquet('/nonexistent-dir-of-this-test/x.parquet')"));
            assertTrue(e.getMessage().startsWith(DuckDbErrors.PENDING_WRAPPER), e.getMessage());
            String line = DuckDbErrors.errorLine(e.getMessage());
            assertTrue(line.startsWith("IO Error: No files found"), line);
        }
    }

    @Test
    void keepsOtherMessagesFirstLine() {
        assertEquals("Parser Error: syntax error at or near \"x\"",
                DuckDbErrors.errorLine("Parser Error: syntax error at or near \"x\"\n\nLINE 1: x"));
        assertEquals("null", DuckDbErrors.errorLine(null));
        assertEquals(DuckDbErrors.PENDING_WRAPPER,
                DuckDbErrors.errorLine(DuckDbErrors.PENDING_WRAPPER + "\nno error line"));
    }
}
