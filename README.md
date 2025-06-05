# flyway-demo
Example usage of the Flyway QuestDB plugin

# Flyway migration via API
```java
public class FlywayTest {
    private static final String PG_HOST = "localhost";
    private static final int PG_PORT = 8812;
    private static final String LOCATION = "questdb_migration";
    private static final String USER = "admin";
    private static final String PWD = "quest";

    public static void main(String[] args) {
        final String targetVersion = "5";

        String jdbcUrl = "jdbc:postgresql://" + PG_HOST + ":" + PG_PORT + "/default";
        Flyway flyway = Flyway
                .configure()
                .locations(LOCATION)
                .dataSource(jdbcUrl, USER, PWD)
                .baselineOnMigrate(true)
                .target(targetVersion)
                .load();
        MigrateResult migrateResult = flyway.migrate();

        if (!migrateResult.getFailedMigrations().isEmpty()) {
            throw new RuntimeException("Migration failed, " + migrateResult.getFailedMigrations().toString());
        }
    }
}
```
