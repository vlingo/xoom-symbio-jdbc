// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc.hsqldb;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicInteger;

import org.hsqldb.server.Server;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.Configuration.ConfigurationInterest;
import io.vlingo.symbio.store.common.jdbc.Configuration.TestConfiguration;

public class HSQLDBConfigurationProvider {
  private static final ConfigurationInterest interest = new ConfigurationInterest() {
    private Server databaseSever;
    private AtomicInteger databaseCount = new AtomicInteger(0);

    @Override
    public void afterConnect(final Connection connection) {

    }

    @Override public void createDatabase(final Connection connection, final String databaseName) {
      databaseCount.incrementAndGet();
    }

    @Override public void dropDatabase(final Connection connection, final String databaseName) {
      boolean isDone = databaseCount.decrementAndGet() == 0;
      if (isDone) {
        databaseSever.shutdown();
        databaseSever = null;
      }
    }

    @Override
    public void beforeConnect(final Configuration configuration) {
      if (databaseSever != null) return;
      databaseSever = new Server();
      databaseSever.start();
    }
  };

  public static Configuration configuration(
          final DataFormat format,
          final String url,
          final String databaseName,
          final String username,
          final String password,
          final String originatorId,
          final boolean createTables)
  throws Exception {
    return new Configuration(
            interest,
            "org.hsqldb.jdbc.JDBCDriver",
            format,
            url,
            databaseName,
            username,
            password,
            false,          // useSSL
            originatorId,
            createTables);
  }

  public static TestConfiguration testConfiguration(final DataFormat format) throws Exception {
    return testConfiguration(format, "testdb");
  }

  public static TestConfiguration testConfiguration(final DataFormat format, final String databaseName) throws Exception {
    return new TestConfiguration(
            interest,
            "org.hsqldb.jdbc.JDBCDriver",
            format,
            "jdbc:hsqldb:mem:",
            databaseName,       // database name
            "SA",           // username
            "",             // password
            false,          // useSSL
            "TEST",         // originatorId
            true);          // create tables
  }
}
