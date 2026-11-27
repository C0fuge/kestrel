package pro.surpin.data.arrowflight.server;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/** Runs selected unchanged upstream tests through a driver that bounds every native connection. */
public final class BoundedUpstreamSmoke {
    /** Prevents instantiation of the test entry point. */
    private BoundedUpstreamSmoke() {
    }

    /**
     * Applies native limits without modifying upstream test methods or SQL.
     *
     * @param args selected upstream filter and fresh evidence directory
     * @throws Exception when registration or upstream execution fails
     */
    public static void main(String[] args) throws Exception {
        Path output = Path.of(args[1]);
        Files.createDirectory(output);
        Class.forName("org.duckdb.DuckDBDriver");
        Driver original = DriverManager.getDriver("jdbc:duckdb:");
        DriverManager.deregisterDriver(original);
        DriverManager.registerDriver(new Driver() {
            @Override
            public Connection connect(String url, Properties supplied) throws SQLException {
                if (!original.acceptsURL(url)) {
                    return null;
                }
                try {
                    Properties bounded = new Properties();
                    bounded.putAll(supplied);
                    bounded.putAll(NativeCompilerSmoke.nativeProperties(output));
                    Connection connection = original.connect(url, bounded);
                    try {
                        NativeCompilerSmoke.verifyNativeLimits(connection, output);
                        return connection;
                    } catch (Exception error) {
                        connection.close();
                        throw error;
                    }
                } catch (Exception error) {
                    throw error instanceof SQLException sql ? sql : new SQLException(error);
                }
            }

            @Override
            public boolean acceptsURL(String url) throws SQLException {
                return original.acceptsURL(url);
            }

            @Override
            public DriverPropertyInfo[] getPropertyInfo(String url, Properties properties) throws SQLException {
                return original.getPropertyInfo(url, properties);
            }

            @Override
            public int getMajorVersion() {
                return original.getMajorVersion();
            }

            @Override
            public int getMinorVersion() {
                return original.getMinorVersion();
            }

            @Override
            public boolean jdbcCompliant() {
                return original.jdbcCompliant();
            }

            @Override
            public Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
                return original.getParentLogger();
            }
        });
        Class<?> upstream = Class.forName("org.duckdb.TestDuckDBJDBC");
        upstream.getMethod("main", String[].class).invoke(null, (Object) new String[] {args[0]});
    }
}
