// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.symbio.store.DataFormat;

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

  public final Connection connection;
  public final String databaseName;
  public final String driverClassname;
  public final DataFormat format;
  public final String url;
  public final String username;
  public final String password;
  public final boolean useSSL;
  public final String originatorId;
  public final boolean createTables;
  public final long transactionTimeoutMillis;

  protected final ConfigurationInterest interest;

  public static Configuration cloneOf(final Configuration other) throws Exception {
    return new Configuration(other.interest, other.driverClassname, other.format,
            other.url, other.databaseName, other.username, other.password,
            other.useSSL, other.originatorId, other.createTables, other.transactionTimeoutMillis);
  }

  public Configuration(
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
    this(interest, driverClassname, format, url, databaseName, username, password,
            useSSL, originatorId, createTables, DefaultTransactionTimeout);
  }

  public Configuration(
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

    this.interest = interest;
    this.driverClassname = driverClassname;
    this.format = format;
    this.databaseName = databaseName;
    this.url = url;
    this.username = username;
    this.password = password;
    this.useSSL = useSSL;
    this.originatorId = originatorId;
    this.createTables = createTables;
    this.transactionTimeoutMillis = transactionTimeoutMillis;
    beforeConnect();
    this.connection = connect(url, databaseName);
    afterConnect();
  }

  protected void afterConnect() throws Exception {
    interest.afterConnect(connection);
  }

  protected void beforeConnect() throws Exception {
    interest.beforeConnect(this);
  }

  protected Connection connect(final String url, final String databaseName) {
    try {
      Class.forName(driverClassname);
      final Properties properties = new Properties();
      properties.setProperty("user", username);
      properties.setProperty("password", password);
      properties.setProperty("ssl", Boolean.toString(useSSL));
      final Connection connection = DriverManager.getConnection(url + databaseName, properties);
      connection.setAutoCommit(false);
      return connection;
    }  catch (Exception e) {
      throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because database unavilable or wrong credentials.");
    }
  }

  public interface ConfigurationInterest {
    void afterConnect(final Connection connection) throws Exception;
    void beforeConnect(final Configuration configuration) throws Exception;
    void createDatabase(final Connection connection, final String databaseName) throws Exception;
    void dropDatabase(final Connection connection, final String databaseName) throws Exception;
  }

  public static class TestConfiguration extends Configuration {
    static private final AtomicInteger uniqueNumber = new AtomicInteger(0);

    private String testDatabaseName;

    public TestConfiguration(
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
      super(interest, driverClassname, format, url, databaseName, username, password, useSSL, originatorId, createTables);
    }

    public void cleanUp() {
      try (final Connection ownerConnection = swapConnections()) {
        try (final Statement statement = ownerConnection.createStatement()) {
          ownerConnection.setAutoCommit(true);
          interest.dropDatabase(ownerConnection, testDatabaseName);
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
    protected Connection connect(final String url, final String databaseName) {
      final Connection connection = super.connect(url, databaseName);

      try (final Statement statement = connection.createStatement()) {
        this.testDatabaseName = testDatabaseName(format, databaseName);
        interest.createDatabase(connection, testDatabaseName);
        connection.close();
        return super.connect(url, testDatabaseName);
      }  catch (Exception e) {
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because the server or database unavilable, or wrong credentials.", e);
      }
    }
    
    protected String testDatabaseName(final DataFormat format, final String databaseName) {
      return databaseName +
              "_" +
              uniqueNumber.incrementAndGet() +
              (format.isBinary() ? "b":"t");
    }

    private Connection swapConnections() {
      try {
        connection.close();
        return super.connect(url, databaseName);
      } catch (Exception e) {
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot swap database to owner's because: " + e.getMessage(), e);
      }
    }
  }
}
