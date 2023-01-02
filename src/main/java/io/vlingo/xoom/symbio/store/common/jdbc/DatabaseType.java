// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.jdbc;

import java.sql.Connection;

/**
 *  Enumerated database types.
 */
public enum DatabaseType {
  HSQLDB, MySQL, MariaDB, Oracle, Postgres, SQLServer, Vitess, YugaByte;

  /**
   * Answer the {@code DatabaseType} given a {@code Connection}.
   * @param connection the Connection
   * @return DatabaseType
   */
  public static DatabaseType databaseType(final Connection connection) {
    String url = "uninitialized-database-url";
    try {
      url = connection.getMetaData().getURL().toLowerCase();
      return databaseType(url);
    } catch (Exception e) {
      // fall through
    }
    throw new IllegalStateException("Unknown database type for: " + url);
  }

  /**
   * Answer the {@code DatabaseType} given a {@code url}.
   * @param url the String URL of the database connection
   * @return DatabaseType
   */
  public static DatabaseType databaseType(final String url) {
    try {
      for (final DatabaseType type : DatabaseType.values()) {
        if (url.contains(type.name().toLowerCase())) {
          return type;
        }
      }
    } catch (Exception e) {
      // fall through
    }
    throw new IllegalStateException("Unknown database type for: " + url);
  }
}
