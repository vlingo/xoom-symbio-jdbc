package io.vlingo.symbio.store.testcontainers;

import org.testcontainers.containers.PostgreSQLContainer;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;

public class SharedPostgreSQLContainer extends PostgreSQLContainer<SharedPostgreSQLContainer> {
    private static final String IMAGE_VERSION = "postgres:latest";
    private static SharedPostgreSQLContainer container;

    private SharedPostgreSQLContainer() {
        super(IMAGE_VERSION);
    }

    @SuppressWarnings("resource")
    public static SharedPostgreSQLContainer getInstance() {
        if (container == null) {
            container = new SharedPostgreSQLContainer()
                    .withDatabaseName("vlingo_test")
                    .withUsername("vlingo_test")
                    .withPassword("vlingo123");
            container.start();
        }
        return container;
    }

    @Override
    public void stop() {
        // do nothing, the JVM handles shut down
    }

    public Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return new Configuration.TestConfiguration(
                DatabaseType.Postgres,
                PostgresConfigurationProvider.interest,
                "org.postgresql.Driver",
                format,
                "jdbc:postgresql://" + getHost() + ":" + getMappedPort(POSTGRESQL_PORT) + "/",
                getDatabaseName(),
                getUsername(),
                getPassword(),
                false,
                "TEST",
                true);
    }
}
