// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.jdbc;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;

/**
 * A standard configuration for JDBC connections used by
 * {@code StateStore}, {@code Journal}, and {@code ObjectStore}.
 */
public class Configuration {

  /**
   * A default timeout for transactions if not specified by the client.
   * Note that this is not used as a database timeout, but rather the
   * timeout between reading some entity/entities from storage and writing
   * and committing it/them back to storage. This means that user think-time
   * is built in to the timeout value. The current default is 5 minutes
   * but can be overridden by using the constructor that accepts the
   * {@code transactionTimeoutMillis} value. For a practical use of this
   * see the following implementations, where this timeout represents the
   * time between creation of the {@code UnitOfWork} and its expiration:
   * <p>
   * {@code io.vlingo.xoom.symbio.store.object.jdbc.jdbi.JdbiObjectStoreDelegate}
   * {@code io.vlingo.xoom.symbio.store.object.jdbc.jdbi.UnitOfWork}
   * </p>
   */
  public static final long DefaultTransactionTimeout = 5 * 60 * 1000L; // 5 minutes

  public static final int DefaultMaxConnections = 3;

  public final String databaseName; // ownerDatabaseName
  public final String actualDatabaseName;
  public final ConnectionProvider connectionProvider;
  public final DatabaseType databaseType;
  public final DataFormat format;
  public final String originatorId;
  public final boolean createTables;
  public final long transactionTimeoutMillis;

  protected final ConfigurationInterest interest;

  public static Configuration cloneOf(final Configuration other) {
    try {
      return new Configuration(other.databaseType, other.interest, other.connectionProvider.driverClassname, other.format,
              other.connectionProvider.url, other.actualDatabaseName, other.connectionProvider.username, other.connectionProvider.password, other.connectionProvider.useSSL,
              other.connectionProvider.maxConnections, other.originatorId, other.createTables, other.transactionTimeoutMillis, true);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot clone the configuration for " + other.connectionProvider.url + " because: " + e.getMessage(), e);
    }
  }

  public static ConfigurationInterest interestOf(final DatabaseType databaseType) {
    switch (databaseType) {
    case HSQLDB:
      return HSQLDBConfigurationProvider.interest;
    case MySQL:
    case MariaDB:
      return MySQLConfigurationProvider.interest;
    case SQLServer:
      break;
    case Vitess:
      break;
    case Oracle:
      break;
    case Postgres:
      return PostgresConfigurationProvider.interest;
    case YugaByte:
      return YugaByteConfigurationProvider.interest;
    }

    throw new IllegalArgumentException("Database currently not supported: " + databaseType.name());
  }

  public Configuration(
          final DatabaseType databaseType,
          final ConfigurationInterest interest,
          final String driverClassname,
          final DataFormat format,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final boolean useSSL,
          final String originatorId,
          final boolean createTables)
    throws Exception {
    this(databaseType, interest, driverClassname, format, url, databaseName, username, password,
            useSSL, originatorId, createTables, DefaultTransactionTimeout);
  }

  public Configuration(
      final DatabaseType databaseType,
      final ConfigurationInterest interest,
      final String driverClassname,
      final DataFormat format,
      final String url,
      final String databaseName,
      final String username,
      final String password,
      final boolean useSSL,
      final int maxConnections,
      final String originatorId,
      final boolean createTables)
      throws Exception {
    this(databaseType, interest, driverClassname, format, url, databaseName, username, password,
        useSSL, maxConnections, originatorId, createTables, DefaultTransactionTimeout, false);
  }

  public Configuration(
          final DatabaseType databaseType,
          final ConfigurationInterest interest,
          final String driverClassname,
          final DataFormat format,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final boolean useSSL,
          final String originatorId,
          final boolean createTables,
          final long transactionTimeoutMillis)
    throws Exception {
    this(databaseType, interest, driverClassname, format, url, databaseName, username, password,
            useSSL, originatorId, createTables, DefaultTransactionTimeout, false);
  }

  public Configuration(
      final DatabaseType databaseType,
      final ConfigurationInterest interest,
      final String driverClassname,
      final DataFormat format,
      final String url,
      final String databaseName,
      final String username,
      final String password,
      final boolean useSSL,
      final String originatorId,
      final boolean createTables,
      final long transactionTimeoutMillis,
      final boolean reuseDatabaseName)
      throws Exception {
    this(databaseType, interest, driverClassname, format, url, databaseName, username, password, useSSL, 5,
        originatorId, createTables, transactionTimeoutMillis, reuseDatabaseName);
  }

  public Configuration(
      final DatabaseType databaseType,
      final ConfigurationInterest interest,
      final String driverClassname,
      final DataFormat format,
      final String url,
      final String databaseName,
      final String username,
      final String password,
      final boolean useSSL,
      final int maxConnections,
      final String originatorId,
      final boolean createTables,
      final long transactionTimeoutMillis,
      final boolean reuseDatabaseName)
      throws Exception {

    this.format = format;
    this.databaseName = databaseName;
    this.actualDatabaseName = reuseDatabaseName ? databaseName : actualDatabaseName(databaseName);
    this.databaseType = databaseType;
    this.interest = interest;
    this.originatorId = originatorId;
    this.createTables = createTables;
    this.transactionTimeoutMillis = transactionTimeoutMillis;

    beforeConnect(); // no Connection is available yet
    this.connectionProvider = connectionProvider(driverClassname, url, databaseName, actualDatabaseName, username, password, useSSL, maxConnections);

    try (final Connection connection = connectionProvider.newConnection()) {
      try {
        afterConnect(connection);
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        throw new IllegalStateException("Failed to initialize Configuration because: " + e.getMessage(), e);
      }
    }
  }

  protected String actualDatabaseName(final String databaseName) {
    return databaseName;
  }

  protected void afterConnect(Connection connection) throws Exception {
    interest.afterConnect(connection);
  }

  protected void beforeConnect() throws Exception {
    interest.beforeConnect(this);
  }

  protected ConnectionProvider connectionProvider(
      final String driverClassname,
      final String url,
      final String databaseName,
      final String actualDatabaseName,
      final String username,
      final String password,
      final boolean useSSL,
      final int maxConnections) {
    return new ConnectionProvider(driverClassname, url, actualDatabaseName, username, password, useSSL, maxConnections);
  }

  public interface ConfigurationInterest {
    void afterConnect(final Connection connection) throws Exception;
    void beforeConnect(final Configuration configuration) throws Exception;
    void createDatabase(final Connection initConnection, final String databaseName, final String username) throws Exception;
    void dropDatabase(final Connection initConnection, final String databaseName) throws Exception;
  }

  public static class TestConfiguration extends Configuration {
    static private final AtomicInteger uniqueNumber = new AtomicInteger(0);

    public TestConfiguration(
            final DatabaseType databaseType,
            final ConfigurationInterest interest,
            final String driverClassname,
            final DataFormat format,
            final String url,
            final String databaseName,
            final String username,
            final String password,
            final boolean useSSL,
            final int maxConnections,
            final String originatorId,
            final boolean createTables)
    throws Exception {
      super(databaseType, interest, driverClassname, format, url, databaseName, username, password, useSSL, maxConnections, originatorId, createTables, DefaultTransactionTimeout, false);
    }

    public void cleanUp() {
      try (final Connection ownerConnection = ConnectionProvider.connectionWith(connectionProvider.driverClassname, connectionProvider.url, databaseName,
          connectionProvider.username, connectionProvider.password, connectionProvider.useSSL)) {
        try {
          connectionProvider.close(); // close all Connections before deleting the database
          interest.dropDatabase(ownerConnection, this.actualDatabaseName);
          ownerConnection.commit();
        } catch (Exception e) {
          ownerConnection.rollback();
          e.printStackTrace();
          // ignore
        }
      } catch (Exception e) {
        e.printStackTrace();
        // ignore
      }
    }

    @Override
    protected String actualDatabaseName(final String databaseName) {
      return databaseName +
              "_" +
              uniqueNumber.incrementAndGet() +
              (format.isBinary() ? "b":"t");
    }

    @Override
    protected ConnectionProvider connectionProvider(
        String driverClassname,
        String url,
        String databaseName,
        String actualDatabaseName,
        String username,
        String password,
        boolean useSSL,
        int maxConnections) {
      try (Connection connection = ConnectionProvider.connectionWith(driverClassname, url, databaseName, username, password, useSSL)) {
        interest.createDatabase(connection, actualDatabaseName, username);
        connection.commit();
        return super.connectionProvider(driverClassname, url, databaseName, actualDatabaseName, username, password, useSSL, maxConnections);
      } catch (Exception e) {
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because the server or database unavailable, or wrong credentials.", e);
      }
    }
  }
}
