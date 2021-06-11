package io.vlingo.xoom.symbio.store.testcontainers;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.HashMap;
import java.util.Map;

public class SharedPostgreSQLContainer extends PostgreSQLContainer<SharedPostgreSQLContainer> {
  private static final String IMAGE_VERSION = "postgres:latest";
  private static SharedPostgreSQLContainer instance;

  private final Map<DataFormat, Configuration.TestConfiguration> configurations = new HashMap<>();

  private SharedPostgreSQLContainer() {
    super(IMAGE_VERSION);
  }

  @SuppressWarnings("resource")
  public static SharedPostgreSQLContainer getInstance() {
    if (instance == null) {
      instance = new SharedPostgreSQLContainer()
          .withDatabaseName("xoom_test")
          .withUsername("xoom_test")
          .withPassword("vlingo123");
      instance.start();
    }
    return instance;
  }

  @Override
  public void stop() {
    // do nothing, the JVM handles shut down
  }

  public Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
//    Configuration.TestConfiguration config = configurations.get(format);
//
//    if (config == null) {
//      config = new Configuration.TestConfiguration(
//          DatabaseType.Postgres,
//          PostgresConfigurationProvider.interest,
//          "org.postgresql.Driver",
//          format,
//          "jdbc:postgresql://" + getHost() + ":" + getMappedPort(POSTGRESQL_PORT) + "/",
//          getDatabaseName(),
//          getUsername(),
//          getPassword(),
//          false,
//          "TEST",
//          true);
//      configurations.put(format, config);
//    }
//
//    return config;

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
