package com.questdb;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;

import java.sql.*;

public class FlywayTest {
    private static final String PG_HOST = "localhost";
    private static final int PG_PORT = 8812;
    private static final String LOCATION = "questdb_migration";
    private static final String USER = "admin";
    private static final String PWD = "quest";
    private static final int TIMEOUT_IN_MILLIS = 10000;

    public static void main(String[] args) {
        runMigration(
                "5",
                "show create table trades",
                "ddl\n" +
                        "CREATE TABLE 'trades' ( \n" +
                        "\tinstrument SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tside SYMBOL CAPACITY 256 CACHE,\n" +
                        "\tqty DOUBLE,\n" +
                        "\tprice DOUBLE,\n" +
                        "\tts TIMESTAMP,\n" +
                        "\tvenue SYMBOL CAPACITY 256 CACHE\n" +
                        ") timestamp(ts) PARTITION BY DAY WAL\n" +
                        "WITH maxUncommittedRows=500000, o3MaxLag=600000000us;\n"
        );
    }

    private static void runMigration(String version, String query, String expectedResult) {
        final String jdbcUrl = jdbcUrl();
        final Flyway flyway = flyway(jdbcUrl, version);
        final MigrateResult migrateResult = flyway.migrate();

        assertMigrations(migrateResult);
        assertQuery(jdbcUrl, query, expectedResult);
    }

    private static void assertMigrations(MigrateResult migrateResult) {
        if (!migrateResult.getFailedMigrations().isEmpty()) {
            throw new RuntimeException("Migration failed, " + migrateResult.getFailedMigrations().toString());
        }
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/default";
    }

    private static Flyway flyway(String jdbcUrl, String version) {
        return Flyway
                .configure()
                .locations(LOCATION)
                .dataSource(jdbcUrl, USER, PWD)
                .baselineOnMigrate(true)
                .target(version)
                .load();
    }

    private static void assertQuery(String jdbcUrl, String query, String expectedResult) {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, USER, PWD)) {
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(query)) {
                final long start = System.currentTimeMillis();
                do {
                    if (expectedResult.equals(resultSetToString(resultSet))) {
                        return;
                    }
                    sleep(1000L);
                } while (System.currentTimeMillis() - start < TIMEOUT_IN_MILLIS);
                throw new RuntimeException("SQL assert failure, expected '" + expectedResult + "', got '" + resultSetToString(resultSet) + "', or timed out");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
        }
    }
    private static String resultSetToString(ResultSet rs) throws SQLException {
        final StringBuilder sb = new StringBuilder();
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnCount = metaData.getColumnCount();

        // Print column headers
        for (int i = 1; i <= columnCount; i++) {
            sb.append(metaData.getColumnName(i));
            if (i < columnCount) {
                sb.append("\t");
            }
        }
        sb.append("\n");

        // Print rows
        while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
                sb.append(rs.getString(i));
                if (i < columnCount) {
                    sb.append("\t");
                }
            }
            sb.append("\n");
        }

        return sb.toString();
    }
}