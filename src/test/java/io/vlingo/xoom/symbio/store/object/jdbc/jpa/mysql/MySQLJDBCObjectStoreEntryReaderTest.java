// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jpa.mysql;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;
import io.vlingo.xoom.symbio.store.object.jdbc.MySQLObjectStoreEntryJournalQueries;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.JDBCObjectStoreEntryReaderTest;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.JPAObjectStoreDelegate;
import io.vlingo.xoom.symbio.store.testcontainers.SharedMySQLContainer;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class MySQLJDBCObjectStoreEntryReaderTest extends JDBCObjectStoreEntryReaderTest {
  private SharedMySQLContainer mysqlContainer = SharedMySQLContainer.getInstance();

  @Override
  protected Configuration createAdminConfiguration() throws Exception {
    return mysqlContainer.testConfiguration(DataFormat.Text);
  }

  @Override
  protected JPAObjectStoreDelegate createDelegate(Map<String, Object> properties, String originatorId, StateAdapterProvider stateAdapterProvider, Logger logger) {
    return new JPAObjectStoreDelegate(JPAObjectStoreDelegate.JPA_MYSQL_PERSISTENCE_UNIT, properties, "TEST", stateAdapterProvider, logger);
  }

  @Override
  protected ConnectionProvider createConnectionProvider() {
    return new ConnectionProvider(
        "com.mysql.cj.jdbc.Driver",
        "jdbc:mysql://" + mysqlContainer.getHost() + ":" + mysqlContainer.getMappedPort(SharedMySQLContainer.MYSQL_PORT) + "/",
        testDatabaseName,
        mysqlContainer.getUsername(),
        mysqlContainer.getPassword(),
        false,
        Configuration.DefaultMaxConnections);
  }

  @Override
  protected JDBCObjectStoreEntryJournalQueries createQueries() {
    return new MySQLObjectStoreEntryJournalQueries();
  }

  @Override
  protected void createTestDatabase() throws Exception {
    try (final Connection connection = adminConfiguration.connectionProvider.newConnection()) {
      try {
        connection.setAutoCommit(true);
        MySQLConfigurationProvider.interest.createDatabase(connection, testDatabaseName, adminConfiguration.connectionProvider.username);
        connection.setAutoCommit(false);
      } catch (Exception e) {
        connection.setAutoCommit(false);
        throw e;
      }
    }
  }

  @Override
  protected void dropTestDatabase() throws Exception {
    try (final Connection connection = adminConfiguration.connectionProvider.newConnection()) {
      try {
        connection.setAutoCommit(true);
        MySQLConfigurationProvider.interest.dropDatabase(connection, testDatabaseName);
        connection.setAutoCommit(false);
      } catch (Exception e) {
        connection.setAutoCommit(false);
        throw e;
      }
    }
  }

  @Override
  protected Map<String, Object> getDatabaseSpecificProperties(String databaseNamePostfix) {
    Map<String, Object> properties = new HashMap<>();

    properties.put("javax.persistence.jdbc.driver", "com.mysql.cj.jdbc.Driver");
    properties.put("javax.persistence.jdbc.url", "jdbc:mysql://" + mysqlContainer.getHost() + ":" + mysqlContainer.getMappedPort(SharedMySQLContainer.MYSQL_PORT) + "/" + databaseNamePostfix);
    properties.put("javax.persistence.jdbc.user", mysqlContainer.getUsername());
    properties.put("javax.persistence.jdbc.password", mysqlContainer.getPassword());

    return properties;
  }
}
