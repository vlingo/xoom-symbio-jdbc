package io.vlingo.xoom.symbio.store.testcontainers;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.utility.DockerImageName;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;

public class SharedYugaByteDbContainer extends JdbcDatabaseContainer<SharedYugaByteDbContainer> {
    public static final Integer YUGABYTE_PORT = 5433;
    private static final String IMAGE_VERSION = "yugabytedb/yugabyte:latest";
    private static SharedYugaByteDbContainer container;

    private SharedYugaByteDbContainer() {
        super(DockerImageName.parse(IMAGE_VERSION));
    }

    @SuppressWarnings("resource")
    public static SharedYugaByteDbContainer getInstance() {
        if (container == null) {
            container = new SharedYugaByteDbContainer()
                    .withCommand("bin/yugabyted start --daemon=false")
                    .withExposedPorts(YUGABYTE_PORT);
            container.start();
        }
        return container;
    }

    @Override
    public void stop() {
        // do nothing, the JVM handles shut down
    }

    @Override
    public String getDriverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String getJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return "jdbc:postgresql://" + getContainerIpAddress() + ":" + getMappedPort(YUGABYTE_PORT) + "/" + getDatabaseName() + additionalUrlParams;
    }

    @Override
    public String getDatabaseName() {
        return "postgres";
    }

    @Override
    public String getUsername() {
        return "postgres";
    }

    @Override
    public String getPassword() {
        return "postgres";
    }

    public Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return new Configuration.TestConfiguration(
                DatabaseType.YugaByte,
                YugaByteConfigurationProvider.interest,
                "org.postgresql.Driver",
                format,
                "jdbc:postgresql://" + getHost() + ":" + getMappedPort(YUGABYTE_PORT) + "/",
                getDatabaseName(),
                getUsername(),
                getPassword(),
                false,
                "TEST",
                true);
    }

    @Override
    protected String getTestQueryString() {
        return "SELECT 1";
    }
}
