// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.jdbc.postgres;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration.ConfigurationInterest;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;

public class PostgresConfigurationProvider {
    public static final ConfigurationInterest interest = new ConfigurationInterest() {
        private Configuration configuration;

        @Override public void afterConnect(final Connection connection) { }

        @Override public void beforeConnect(final Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public void createDatabase(final Connection initConnection, final String databaseName) throws Exception {
            try (final Statement statement = initConnection.createStatement()) {
                initConnection.setAutoCommit(true);
                statement.executeUpdate("CREATE DATABASE " + databaseName + " WITH OWNER = " + configuration.connectionProvider.username);
                initConnection.setAutoCommit(false);
            } catch (Exception e) {
                final List<String> message = Arrays.asList(e.getMessage().split(" "));
                if (message.contains("database") && message.contains("already") && message.contains("exists")) return;
                System.out.println("Postgres database " + databaseName + " could not be created because: " + e.getMessage());

                throw e;
            }
        }

        @Override
        public void dropDatabase(final Connection initConnection, final String databaseName) throws Exception {
            try (final Statement statement = initConnection.createStatement()) {
                initConnection.setAutoCommit(true);
                statement.executeUpdate("DROP DATABASE " + databaseName);
                initConnection.setAutoCommit(false);
            } catch (Exception e) {
                System.out.println("Postgres database " + databaseName + " could not be dropped because: " + e.getMessage());
            }
        }
    };

    public static Configuration configuration(
            final DataFormat format,
            final String url,
            final String databaseName,
            final String username,
            final String password,
            final String originatorId,
            final boolean createTables) throws Exception {
        return new Configuration(
                DatabaseType.Postgres,
                interest,
                "org.postgresql.Driver",
                format,
                url,
                databaseName,
                username,
                password,
                false,
                originatorId,
                createTables);
    }
}
