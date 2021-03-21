package io.vlingo.symbio.store.testcontainers;

import java.sql.Connection;
import java.sql.SQLException;

import org.testcontainers.containers.MySQLContainer;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;

public class SharedMySQLContainer extends MySQLContainer<SharedMySQLContainer> {
    private static final String IMAGE_VERSION = "mysql:latest";
    private static SharedMySQLContainer container;

    private SharedMySQLContainer() {
        super(IMAGE_VERSION);
    }

    @SuppressWarnings("resource")
    public static SharedMySQLContainer getInstance() {
        if (container == null) {
            String username = "vlingo_test";
            String databaseName = "vlingo_test";
            String password = "vlingo123";
            container = new SharedMySQLContainer()
                    .withEnv("MYSQL_ROOT_HOST", "%")
                    .withDatabaseName(databaseName)
                    .withUsername("root")
                    .withPassword("");
            container.start();
            createUser(username, password);
            container.withUsername(username).withPassword(password);
        }
        return container;
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
                "TEST",
                true);
    }

    private static void createUser(String username, String password) {
        try(Connection connection = container.createConnection("")) {
            connection.createStatement().executeUpdate(String.format("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';", username, password));
            connection.createStatement().executeUpdate(String.format("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';", username));
        } catch (SQLException cause) {
            throw new RuntimeException("Failed to create the test user.", cause);
        }
    }
}
