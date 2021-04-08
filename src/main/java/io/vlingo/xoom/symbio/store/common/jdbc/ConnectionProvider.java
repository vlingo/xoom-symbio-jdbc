// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.jdbc;

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

  final String password;

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
  }

  /**
   * Answer a new instance of a {@code Connection}.
   * @return Connection
   */
  public Connection connection() {
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
      throw new IllegalStateException(getClass().getSimpleName() + ": Cannot connect because database unavailable or wrong credentials.");
    }
  }

  /**
   * Answer a copy of me but with the given {@code databaseName}.
   * @param databaseName the String name of the database with which to create the new ConnectionProvider
   * @return ConnectionProvider
   */
  public ConnectionProvider copyReplacing(final String databaseName) {
    return new ConnectionProvider(driverClassname, url, databaseName, username, password, useSSL);
  }
}
