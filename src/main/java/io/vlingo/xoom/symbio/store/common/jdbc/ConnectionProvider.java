// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
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

/**
 * Provider of {@code Connection} instances.
 */
public class ConnectionProvider {
  public final String databaseName;
  public final String driverClassname;
  public final String url;
  public final String username;
  public final boolean useSSL;

  final String password;

  private final DataSource dataSource;

  public ConnectionProvider(
          final String driverClassname,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final boolean useSSL) {
    this.driverClassname = driverClassname;
    this.url = url;
    this.databaseName = databaseName;
    this.username = username;
    this.password = password;
    this.useSSL = useSSL;

    dataSource = buildDataSource();
  }

  /**
   * Answer a new instance of a {@code Connection}.
   * @return Connection
   */
  public Connection connection() {
    try {
      return dataSource.getConnection();
    }  catch (Exception e) {
      throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because database unavailable or wrong credentials.");
    }
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
   * Answer a copy of me but with the given {@code databaseName}.
   * @param databaseName the String name of the database with which to create the new ConnectionProvider
   * @return ConnectionProvider
   */
  public ConnectionProvider copyReplacing(final String databaseName) {
    return new ConnectionProvider(driverClassname, url, databaseName, username, password, useSSL);
  }

  private DataSource buildDataSource() {
    HikariConfig config = new HikariConfig();
    config.setDriverClassName(driverClassname);
    config.setUsername(username);
    config.setPassword(password);
    config.setJdbcUrl(url + databaseName);
    config.setMaximumPoolSize(5);
    config.setAutoCommit(false);

    return new HikariDataSource(config);
  }
}
