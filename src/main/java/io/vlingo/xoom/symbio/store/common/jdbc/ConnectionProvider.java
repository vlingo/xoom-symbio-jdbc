// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Provider of {@code Connection} instances.
 */
public class ConnectionProvider {
  public final String databaseName;
  public final String driverClassname;
  public final String url;
  public final String username;
  public final boolean useSSL;
  public final int maxConnections;

  final String password;

  private final HikariDataSource dataSource;

  public ConnectionProvider(
          final String driverClassname,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final boolean useSSL,
          final int maxConnections) {
    this.driverClassname = driverClassname;
    this.url = url;
    this.databaseName = databaseName;
    this.username = username;
    this.password = password;
    this.useSSL = useSSL;
    this.maxConnections = maxConnections;

    dataSource = buildDataSource();
  }

  /**
   * Close all underlying resources. This method is used on graceful shutdown or unit tests.
   */
  public void close() {
    dataSource.close();
  }

  /**
   * Answer my {@link DataSource}.
   *
   * @return
   */
  public DataSource dataSource() {
    return dataSource;
  }

  /**
   * Answer a new instance of a {@code Connection}.
   * @return Connection
   */
  public Connection newConnection() {
    try {
      return dataSource.getConnection();
    }  catch (Exception e) {
      throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because database unavailable or wrong credentials.");
    }
  }

  /**
   * Answer an individual {@link Connection} which is not handled by connection pool.
   * @param driverClassname
   * @param url
   * @param databaseName
   * @param username
   * @param password
   * @param useSSL
   * @return
   */
  protected static Connection connectionWith(
      final String driverClassname,
      final String url,
      final String databaseName,
      final String username,
      final String password,
      final boolean useSSL) {
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
      throw new IllegalStateException("Cannot connect because database unavailable or wrong credentials.");
    }
  }

  private HikariDataSource buildDataSource() {
    HikariConfig config = new HikariConfig();
    config.setDriverClassName(driverClassname);
    config.setUsername(username);
    config.setPassword(password);
    config.setJdbcUrl(url + databaseName);
    config.setMaximumPoolSize(maxConnections);
    config.setAutoCommit(false);

    return new HikariDataSource(config);
  }
}
