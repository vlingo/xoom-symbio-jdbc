// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class JDBCDispatcherControlDelegate implements DispatcherControl.DispatcherControlDelegate<Entry<String>, State.TextState> {
  static final String DISPATCHEABLE_ENTRIES_DELIMITER = "|";

  private final ConnectionProvider connectionProvider;
  private final DatabaseType databaseType;
  private final Logger logger;
  private final JDBCQueries queries;
  private final String originatorId;

  public JDBCDispatcherControlDelegate(final Configuration configuration, final Logger logger) throws SQLException {
    this.connectionProvider = configuration.connectionProvider;
    this.databaseType = configuration.databaseType;
    this.logger = logger;
    this.originatorId = configuration.originatorId;

    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try {
        this.queries = JDBCQueries.queriesFor(connection);
        queries.createTables(connection);
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        String message = "Failed to initialize JDBCDispatcherControlDelegate because: " + e.getMessage();
        logger.error(message, e);
        throw new IllegalStateException(message, e);
      }
    }
  }

  @Override
  public Collection<Dispatchable<Entry<String>, State.TextState>> allUnconfirmedDispatchableStates() throws Exception {
    final List<Dispatchable<Entry<String>, State.TextState>> dispatchables = new ArrayList<>();

    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement selectDispatchables = queries.prepareSelectDispatchablesQuery(connection, originatorId);
           final ResultSet result = selectDispatchables.executeQuery()) {
        while (result.next()) {
          dispatchables.add(dispatchableFrom(result));
        }
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger.error("xoom-symbio-jdbc-" + databaseType + " error: Failed to query all unconfirmed dispatchables because: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger.error("xoom-symbio-jdbc-" + databaseType + " connection error: Unexpected error occurred when querying all unconfirmed dispatchables because: " + e.getMessage(), e);
    }

    return dispatchables;
  }

  @Override
  public void confirmDispatched(final String dispatchId) {
    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement deleteDispatchable = queries.prepareDeleteDispatchableQuery(connection, dispatchId)) {
        deleteDispatchable.executeUpdate();
        connection.commit();
      } catch (final Exception e) {
        connection.rollback();
        logger.error("xoom-symbio-jdbc-" + databaseType + " error: Failed to confirm dispatch with id " + dispatchId + " because: " + e.getMessage(), e);
      }
    } catch (final Exception e) {
      logger.error("xoom-symbio-jdbc-" + databaseType + " connection error: Unexpected error occurred when confirming dispatch with id " + dispatchId + " because: " + e.getMessage(), e);
    }
  }

  @Override
  public void stop() {
    // no resources to be closed
  }

  private Dispatchable<Entry<String>, State.TextState> dispatchableFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {

    final String dispatchId = resultSet.getString(1);

    LocalDateTime createdOn =
        LocalDateTime.ofInstant(
            Instant.ofEpochMilli(resultSet.getLong(2)),
            ZoneId.systemDefault());

    final State.TextState state;

    final String stateId = resultSet.getString(3);

    if (stateId != null && !stateId.isEmpty()) {
      final String data = resultSet.getString(4);
      final int dataVersion = resultSet.getInt(5);
      final Class<?> type = StoredTypes.forName(resultSet.getString(6));
      final int typeVersion = resultSet.getInt(7);
      final String metadataValue = resultSet.getString(8);
      final Metadata metadata = JsonSerialization.deserialized(metadataValue, Metadata.class);

      state = new State.TextState(stateId, type, typeVersion, data, dataVersion, metadata);
    } else {
      state = null;
    }

    final String entriesIds = resultSet.getString(9);
    final List<Entry<String>> entries = new ArrayList<>();
    if (entriesIds != null && !entriesIds.isEmpty()) {
      final String[] ids = entriesIds.split("\\" + DISPATCHEABLE_ENTRIES_DELIMITER);

      try (final Connection connection = connectionProvider.newConnection()) {
        try (final PreparedStatement selectEntry = queries.prepareSelectEntryQuery(connection)) {
          for (final String entryId : ids) {
            selectEntry.clearParameters();
            selectEntry.setLong(1, Long.parseLong(entryId));

            try (final ResultSet result = selectEntry.executeQuery()) {
              if (result.next()) {
                entries.add(entryFrom(result));
              }
            }
          }

          connection.commit();
        } catch (Exception e) {
          connection.rollback();
          logger.error("xoom-symbio-jdbc-" + databaseType + " error: Failed to construct dispatchable with id " + dispatchId + " because: " + e.getMessage(), e);
        }
      }
    }

    return new Dispatchable<>(dispatchId, createdOn, state, entries);
  }

  private Entry<String> entryFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {
    final String id = resultSet.getString(1);
    final String entryData = resultSet.getString(2);
    final String entryType = resultSet.getString(3);
    final int eventTypeVersion = resultSet.getInt(4);
    final String entryMetadata = resultSet.getString(5);
    final int entryVersion = resultSet.getInt(6); // from E_STREAM_VERSION

    final Class<?> classOfEvent = StoredTypes.forName(entryType);

    final Metadata metadata = JsonSerialization.deserialized(entryMetadata, Metadata.class);
    return new BaseEntry.TextEntry(id, classOfEvent, eventTypeVersion, entryData, entryVersion, metadata);
  }
}
