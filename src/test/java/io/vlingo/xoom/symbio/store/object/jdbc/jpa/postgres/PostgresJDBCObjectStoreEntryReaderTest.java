// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jpa.postgres;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;
import io.vlingo.xoom.symbio.store.object.jdbc.PostgresObjectStoreEntryJournalQueries;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.JDBCObjectStoreEntryReaderTest;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.JPAObjectStoreDelegate;
import io.vlingo.xoom.symbio.store.testcontainers.SharedPostgreSQLContainer;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class PostgresJDBCObjectStoreEntryReaderTest extends JDBCObjectStoreEntryReaderTest {
  private SharedPostgreSQLContainer postgresContainer = SharedPostgreSQLContainer.getInstance();

  @Override
  protected Configuration createAdminConfiguration() throws Exception {
    return postgresContainer.testConfiguration(DataFormat.Text);
  }

  @Override
  protected JPAObjectStoreDelegate createDelegate(Map<String, Object> properties, String originatorId,
                                                  StateAdapterProvider stateAdapterProvider, Logger logger) {
    return new JPAObjectStoreDelegate(JPAObjectStoreDelegate.JPA_POSTGRES_PERSISTENCE_UNIT, properties, "TEST",
        stateAdapterProvider, logger);
  }

  @Override
  protected ConnectionProvider createConnectionProvider() {
    return new ConnectionProvider(
        "org.postgresql.Driver",
        "jdbc:postgresql://" + postgresContainer.getHost() + ":" + postgresContainer.getMappedPort(SharedPostgreSQLContainer.POSTGRESQL_PORT) + "/",
        testDatabaseName,
        postgresContainer.getUsername(),
        postgresContainer.getPassword(),
        false,
        Configuration.DefaultMaxConnections);
  }

  @Override
  protected JDBCObjectStoreEntryJournalQueries createQueries() {
    return new PostgresObjectStoreEntryJournalQueries();
  }

  @Override
  protected void createTestDatabase() throws Exception {
    try (final Connection initConnection = adminConfiguration.connectionProvider.newConnection()) {
      try {
        initConnection.setAutoCommit(true);
        PostgresConfigurationProvider.interest.createDatabase(initConnection, testDatabaseName, adminConfiguration.connectionProvider.username);
        initConnection.setAutoCommit(false);
      } catch (Exception e) {
        initConnection.setAutoCommit(false);
        throw e;
      }
    }
  }

  @Override
  protected void dropTestDatabase() throws Exception {
    try (final Connection initConnection = adminConfiguration.connectionProvider.newConnection()) {
      try {
        initConnection.setAutoCommit(true);
        PostgresConfigurationProvider.interest.dropDatabase(initConnection, testDatabaseName);
        initConnection.setAutoCommit(false);
      } catch (Exception e) {
        initConnection.setAutoCommit(false);
        throw e;
      }
    }
  }

  @Override
  protected Map<String, Object> getDatabaseSpecificProperties(String databaseNamePostfix) {
    Map<String, Object> properties = new HashMap<>();

    properties.put("javax.persistence.jdbc.driver", "org.postgresql.Driver");
    properties.put("javax.persistence.jdbc.url", "jdbc:postgresql://" + postgresContainer.getHost() + ":" + postgresContainer.getMappedPort(SharedPostgreSQLContainer.POSTGRESQL_PORT) + "/" + databaseNamePostfix);
    properties.put("javax.persistence.jdbc.user", postgresContainer.getUsername());
    properties.put("javax.persistence.jdbc.password", postgresContainer.getPassword());

    return properties;
  }
}
