// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.actors.Logger;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.DispatcherControl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class PostgresDispatcherControlDelegate implements DispatcherControl.DispatcherControlDelegate<Entry<String>, State.TextState> {
  static final String DISPATCHEABLE_ENTRIES_DELIMITER = "|";

  private final static String DISPATCHABLE_DELETE = "DELETE FROM vlingo_symbio_journal_dispatch WHERE d_dispatch_id = ?";

  private final static String DISPATCHABLE_SELECT =
          "SELECT d_created_at, d_originator_id, d_dispatch_id, \n" +
                  " d_state_id, d_state_type, d_state_type_version, \n" +
                  " d_state_data, d_state_data_version, \n" +
                  " d_state_metadata, d_entries \n" +
                  " FROM vlingo_symbio_journal_dispatch \n" +
                  " WHERE d_originator_id = ? ORDER BY D_ID";

  private static final String QUERY_ENTRY =
          "SELECT id, entry_data, entry_metadata, entry_type, entry_type_version, entry_timestamp FROM " +
                  "vlingo_symbio_journal WHERE id = ?";

  private final Connection connection;
  private final Logger logger;
  private final PreparedStatement deleteDispatchable;
  private final PreparedStatement selectDispatchables;
  private final PreparedStatement queryEntry;

  PostgresDispatcherControlDelegate(final Connection connection, final String originatorId, final Logger logger) throws SQLException {
    this.connection = connection;
    this.logger = logger;

    this.deleteDispatchable = this.connection.prepareStatement(DISPATCHABLE_DELETE);
    this.selectDispatchables = this.connection.prepareStatement(DISPATCHABLE_SELECT);
    this.selectDispatchables.setString(1, originatorId);

    this.queryEntry = this.connection.prepareStatement(QUERY_ENTRY);
  }

  @Override
  public Collection<Dispatchable<Entry<String>, State.TextState>> allUnconfirmedDispatchableStates() throws Exception {
    final List<Dispatchable<Entry<String>, State.TextState>> dispatchables = new ArrayList<>();

    try (final ResultSet result = selectDispatchables.executeQuery()) {
      while (result.next()) {
        dispatchables.add(dispatchableFrom(result));
      }
    }

    return dispatchables;
  }

  @Override
  public void confirmDispatched(final String dispatchId) {
    try {
      this.deleteDispatchable.clearParameters();
      this.deleteDispatchable.setString(1, dispatchId);
      this.deleteDispatchable.executeUpdate();
      doCommit();
    } catch (final Exception e) {
      logger.error("vlingo/symbio-jdbc-postgres: Failed to confirm dispatch with id" + dispatchId, e);
      fail();
    }
  }

  @Override
  public void stop() {
    try {
      this.deleteDispatchable.close();
    } catch (final SQLException e) {
      //ignore
    }
    try {
      this.selectDispatchables.close();
    } catch (final SQLException e) {
      //ignore
    }
  }

  private void doCommit() {
    try {
      connection.commit();
    } catch (final SQLException e) {
      logger.error("vlingo/symbio-jdbc-postgres: Could not complete transaction", e);
      throw new IllegalStateException(e);
    }
  }

  private void fail() {
    try {
      connection.rollback();
    } catch (final Exception e) {
      logger.error(getClass().getSimpleName() + ": Rollback failed because: " + e.getMessage(), e);
    }
  }

  private Dispatchable<Entry<String>, State.TextState> dispatchableFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {

    final LocalDateTime createdAt = resultSet.getTimestamp(1).toLocalDateTime();
    final String dispatchId = resultSet.getString(3);

    final State.TextState state;
    final String stateId = resultSet.getString(4);
    if (stateId != null && !stateId.isEmpty()) {
      final Class<?> type = Class.forName(resultSet.getString(5));
      final int typeVersion = resultSet.getInt(6);
      // 6 below
      final String data = resultSet.getString(7);

      final int dataVersion = resultSet.getInt(8);
      final String metadataValue = resultSet.getString(9);

      final Metadata metadata = JsonSerialization.deserialized(metadataValue, Metadata.class);

      state = new State.TextState(stateId, type, typeVersion, data, dataVersion, metadata);
    } else {
      state = null;
    }

    final String entriesIds = resultSet.getString(10);
    final List<Entry<String>> entries = new ArrayList<>();
    if (entriesIds != null && !entriesIds.isEmpty()) {
      final String[] ids = entriesIds.split("\\" + DISPATCHEABLE_ENTRIES_DELIMITER);
      for (final String entryId : ids) {
        queryEntry.clearParameters();
        queryEntry.setObject(1, UUID.fromString(entryId));
        queryEntry.executeQuery();
        try (final ResultSet result = queryEntry.executeQuery()) {
          if (result.next()) {
            entries.add(entryFrom(result));
          }
        }
      }
    }
    return new Dispatchable<>(dispatchId, createdAt, state, entries);
  }

  private Entry<String> entryFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {
    final String id = resultSet.getString(1);
    final String entryData = resultSet.getString(2);
    final String entryMetadata = resultSet.getString(3);
    final String entryType = resultSet.getString(4);
    final int eventTypeVersion = resultSet.getInt(5);

    final Class<?> classOfEvent = Class.forName(entryType);

    final Metadata metadata = JsonSerialization.deserialized(entryMetadata, Metadata.class);
    return new BaseEntry.TextEntry(id, classOfEvent, eventTypeVersion, entryData, metadata);
  }
}
