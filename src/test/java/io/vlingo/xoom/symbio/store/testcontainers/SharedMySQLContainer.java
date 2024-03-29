package io.vlingo.xoom.symbio.store.testcontainers;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SharedMySQLContainer extends MySQLContainer<SharedMySQLContainer> {
  private static final String IMAGE_VERSION = "mysql:latest";
  private static SharedMySQLContainer instance;

  private SharedMySQLContainer() {
    super(IMAGE_VERSION);
  }

  @SuppressWarnings("resource")
  public static SharedMySQLContainer getInstance() {
    if (instance == null) {
      String username = "xoom_test";
      String databaseName = "xoom_test";
      String password = "vlingo123";
      instance = new SharedMySQLContainer()
          .withEnv("MYSQL_ROOT_HOST", "%")
          .withDatabaseName(databaseName)
          .withPassword(password);
      instance.start();
      instance.createUser(username, password);
      instance.withUsername(username);
    }
    return instance;
  }

  @Override
  public void stop() {
    // do nothing, the JVM handles shut down
  }

  public Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
    return new Configuration.TestConfiguration(
        DatabaseType.MySQL,
        MySQLConfigurationProvider.interest,
        "com.mysql.cj.jdbc.Driver",
        format,
        "jdbc:mysql://" + getHost() + ":" + getMappedPort(MYSQL_PORT) + "/",
        getDatabaseName(),
        getUsername(),
        getPassword(),
        false,
        Configuration.DefaultMaxConnections,
        "TEST",
        true);
  }

  private void createUser(String username, String password) {
    try (Connection connection = DriverManager.getConnection(getJdbcUrl(), "root", getPassword())) {
      connection.createStatement().executeUpdate(String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", username, password));
      connection.createStatement().executeUpdate(String.format("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';", username));
    } catch (SQLException cause) {
      throw new RuntimeException("Failed to create the test user.", cause);
    }
  }
}
