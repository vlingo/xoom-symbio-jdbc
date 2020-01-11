// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;
import io.vlingo.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;

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
   * {@code io.vlingo.symbio.store.object.jdbc.jdbi.JdbiObjectStoreDelegate}
   * {@code io.vlingo.symbio.store.object.jdbc.jdbi.UnitOfWork}
   * </p>
   */
  public static final long DefaultTransactionTimeout = 5 * 60 * 1000L; // 5 minutes

  public final String actualDatabaseName;
  public final Connection connection;
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
              other.originatorId, other.createTables, other.transactionTimeoutMillis, true);
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
          final String originatorId,
          final boolean createTables,
          final long transactionTimeoutMillis)
    throws Exception {
    this(databaseType, interest, driverClassname, format, url, databaseName, username, password,
            useSSL, originatorId, createTables, DefaultTransactionTimeout, false);
  }

  private Configuration(
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

    this.databaseType = databaseType;
    this.interest = interest;
    this.format = format;
    this.connectionProvider = new ConnectionProvider(driverClassname, url, databaseName, username, password, useSSL);
    this.actualDatabaseName = reuseDatabaseName ? databaseName : actualDatabaseName(databaseName);
    this.originatorId = originatorId;
    this.createTables = createTables;
    this.transactionTimeoutMillis = transactionTimeoutMillis;
    beforeConnect();
    this.connection = connect();
    afterConnect();
  }

  protected String actualDatabaseName(final String databaseName) {
    return connectionProvider.databaseName;
  }

  protected void afterConnect() throws Exception {
    interest.afterConnect(connection);
  }

  protected void beforeConnect() throws Exception {
    interest.beforeConnect(this);
  }

  protected Connection connect() {
    return connectionProvider.connection();
  }

  public interface ConfigurationInterest {
    void afterConnect(final Connection connection) throws Exception;
    void beforeConnect(final Configuration configuration) throws Exception;
    void createDatabase(final Connection connection, final String databaseName) throws Exception;
    void dropDatabase(final Connection connection, final String databaseName) throws Exception;
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
            final String originatorId,
            final boolean createTables)
    throws Exception {
      super(databaseType, interest, driverClassname, format, url, databaseName, username, password, useSSL, originatorId, createTables);
    }

    public void cleanUp() {
      try (final Connection ownerConnection = swapConnections()) {
        try (final Statement statement = ownerConnection.createStatement()) {
          ownerConnection.setAutoCommit(true);
          interest.dropDatabase(ownerConnection, actualDatabaseName);
        }  catch (Exception e) {
          e.printStackTrace();
          // ignore
        }
      }  catch (Exception e) {
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
    protected Connection connect() {
      final Connection connection = super.connect();

      try (final Statement statement = connection.createStatement()) {
        interest.createDatabase(connection, actualDatabaseName);
        connection.close();
        final ConnectionProvider copy = connectionProvider.copyReplacing(actualDatabaseName);
        return copy.connection();
      }  catch (Exception e) {
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because the server or database unavilable, or wrong credentials.", e);
      }
    }

    private Connection swapConnections() {
      try {
        connection.close();
        return connectionProvider.connection();
      } catch (Exception e) {
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot swap database to owner's because: " + e.getMessage(), e);
      }
    }
  }
}
