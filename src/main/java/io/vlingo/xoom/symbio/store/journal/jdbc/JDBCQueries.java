// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.journal.jdbc.mysql.MySQLQueries;
import io.vlingo.xoom.symbio.store.journal.jdbc.postgres.PostgresQueries;
import io.vlingo.xoom.symbio.store.journal.jdbc.postgres.yugabyte.YugaByteQueries;

import java.sql.*;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class JDBCQueries {

  /**
   * Answer a new {@code PostgresQueries} per the {@code DatabaseType} of the {@code connection}.
   * @param initConnection the Connection to use
   * @return PostgresQueries
   * @throws SQLException if the specific PostgresQueries cannot be created
   */
  public static JDBCQueries queriesFor(final Connection initConnection) throws SQLException {
    final DatabaseType databaseType = DatabaseType.databaseType(initConnection);

    switch (databaseType) {
      case Postgres:
        return new PostgresQueries();
      case YugaByte:
        return new YugaByteQueries();
      case MySQL:
        return new MySQLQueries();
      default:
        throw new IllegalArgumentException("Database type not supported: " + databaseType);
    }
  }

  public void createTables(final Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(createJournalTableQuery());
      connection.commit();
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute(createOffsetsTable());
      connection.commit();
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute(createSnapshotsTableQuery());
      connection.commit();
    }

    try (Statement statement = connection.createStatement()) {
      statement.execute(createDispatchableTable());
      connection.commit();
    }
  }

  public void dropTables(final Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(dropDispatchablesTableQuery())) {
      statement.execute();
      connection.commit();
    }

    try (PreparedStatement statement = connection.prepareStatement(dropDispatchablesTableQuery())) {
      statement.execute();
      connection.commit();
    }

    try (PreparedStatement statement = connection.prepareStatement(dropOffsetsTable())) {
      statement.execute();
      connection.commit();
    }

    try (PreparedStatement statement = connection.prepareStatement(dropJournalTable())) {
      statement.execute();
      connection.commit();
    }
  }

  public PreparedStatement prepareDeleteDispatchableQuery(final Connection connection, final String dispatchableId) throws SQLException {
    PreparedStatement deleteDispatchable = connection.prepareStatement(deleteDispatchableQuery());
    deleteDispatchable.clearParameters();
    deleteDispatchable.setString(1, dispatchableId);

    return deleteDispatchable;
  }

  public long generatedKeyFrom(PreparedStatement insertStatement) throws SQLException {
    try (final ResultSet result = insertStatement.getGeneratedKeys()) {
      if (result.next()) {
        return result.getLong(1);
      }
      return -1L;
    }
  }

  public Tuple2<PreparedStatement,Optional<String>> prepareInsertDispatchableQuery(
      final Connection connection,
      final String d_dispatch_id,
      final String d_originator_id,
      final String d_state_id,
      final String d_state_data,
      final int d_state_data_version,
      final String d_state_type,
      final int d_state_type_version,
      final String d_state_metadata,
      final String d_entries)
      throws SQLException {
    PreparedStatement insertDispatchable = newInsertDispatchableQuery(connection);
    updateInsertDispatchableQuery(insertDispatchable, d_dispatch_id, d_originator_id, d_state_id, d_state_data, d_state_data_version,
        d_state_type, d_state_type_version, d_state_metadata, d_entries);

    return Tuple2.from(insertDispatchable, Optional.empty());
  }

  public void updateInsertDispatchableQuery(
      final PreparedStatement insertDispatchable,
      final String d_dispatch_id,
      final String d_originator_id,
      final String d_state_id,
      final String d_state_data,
      final int d_state_data_version,
      final String d_state_type,
      final int d_state_type_version,
      final String d_state_metadata,
      final String d_entries)
      throws SQLException {
    insertDispatchable.clearParameters();

    insertDispatchable.setString(1, d_dispatch_id);
    insertDispatchable.setString(2, d_originator_id);
    insertDispatchable.setLong(3, System.currentTimeMillis());

    insertDispatchable.setString(4, d_state_id);
    insertDispatchable.setString(5, d_state_data);
    insertDispatchable.setInt(6, d_state_data_version);
    insertDispatchable.setString(7, d_state_type);
    insertDispatchable.setInt(8, d_state_type_version);
    insertDispatchable.setString(9, d_state_metadata);
    insertDispatchable.setString(10, d_entries);
  }

  public Tuple2<PreparedStatement, Optional<String>> prepareInsertEntryQuery(
      final Connection connection,
      final String stream_name,
      final int stream_version,
      final String entry_data,
      final String entry_type,
      final int entry_type_version,
      final String entry_metadata)
      throws SQLException {
    PreparedStatement insertEntry = newInsertEntryStatementQuery(connection);
    updateInsertEntryQuery(insertEntry, stream_name, stream_version, entry_data, entry_type, entry_type_version, entry_metadata);

    return Tuple2.from(insertEntry, Optional.empty());
  }

  public void updateInsertEntryQuery(
      PreparedStatement insertEntry,
      final String stream_name,
      final int stream_version,
      final String entry_data,
      final String entry_type,
      final int entry_type_version,
      final String entry_metadata)
      throws SQLException {
    insertEntry.clearParameters();

    insertEntry.setString(1, stream_name);
    insertEntry.setInt(2, stream_version);

    insertEntry.setString(3, entry_data);
    insertEntry.setString(4, entry_type);
    insertEntry.setInt(5, entry_type_version);

    insertEntry.setString(6, entry_metadata);
  }

  public PreparedStatement newInsertDispatchableQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(insertDispatchableQuery());
  }

  public PreparedStatement prepareInsertOffsetQuery(
      final Connection connection,
      final String readerName,
      final long readerOffset)
      throws SQLException {
    PreparedStatement insertOffset = connection.prepareStatement(insertOffsetQuery());
    insertOffset.clearParameters();

    insertOffset.setString(1, readerName);
    insertOffset.setLong(2, readerOffset);

    return insertOffset;
  }

  public Tuple2<PreparedStatement,Optional<String>> prepareInsertSnapshotQuery(
      final Connection connection,
      final String stream_name,
      final int stream_version,
      final String e_snapshot_data,
      final int e_snapshot_data_version,
      final String e_snapshot_type,
      final int e_snapshot_type_version,
      final String e_snapshot_metadata)
      throws SQLException {
    PreparedStatement insertSnapshot = connection.prepareStatement(insertSnapshotQuery());
    insertSnapshot.clearParameters();

    insertSnapshot.setString(1, stream_name);
    insertSnapshot.setInt(2, stream_version);

    insertSnapshot.setString(3, e_snapshot_data);
    insertSnapshot.setInt(4, e_snapshot_data_version);

    insertSnapshot.setString(5, e_snapshot_type);
    insertSnapshot.setInt(6, e_snapshot_type_version);

    insertSnapshot.setString(7, e_snapshot_metadata);

    return Tuple2.from(insertSnapshot, Optional.empty());
  }

  public PreparedStatement prepareSelectCurrentOffsetQuery(final Connection connection, final String readerName) throws SQLException {
    PreparedStatement selectCurrentOffset = connection.prepareStatement(selectCurrentOffset());
    selectCurrentOffset.clearParameters();

    selectCurrentOffset.setString(1, readerName);

    return selectCurrentOffset;
  }

  public PreparedStatement prepareSelectDispatchablesQuery(final Connection connection, final String oringinatorId) throws SQLException {
    PreparedStatement selectDispatchables = connection.prepareStatement(selectDispatchablesQuery());
    selectDispatchables.clearParameters();

    selectDispatchables.setString(1, oringinatorId);

    return selectDispatchables;
  }

  public PreparedStatement prepareSelectEntryQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(selectEntryQuery());
  }

  public PreparedStatement prepareSelectEntryQuery(final Connection connection, final long entryId) throws SQLException {
    PreparedStatement selectEntry = prepareSelectEntryQuery(connection);
    selectEntry.clearParameters();

    selectEntry.setLong(1, entryId);

    return selectEntry;
  }

  public PreparedStatement prepareSelectEntryBatchQuery(final Connection connection, final long entryId, final int count) throws SQLException {
    PreparedStatement selectEntryBatch = connection.prepareStatement(selectEntryBatchQuery());
    selectEntryBatch.clearParameters();

    selectEntryBatch.setLong(1, entryId);
    selectEntryBatch.setLong(2, entryId + count - 1);

    return selectEntryBatch;
  }

  /**
   * Prepare always a new {@link PreparedStatement} which contains SELECT query of entries based on ids.
   *
   * @param connection
   * @param ids the {@code List<Long>} of identities to use in the query
   * @return a {@link PreparedStatement} which needs to be closed due to variable size of ids.
   * @throws SQLException if the statement creation fails
   */
  public PreparedStatement prepareNewSelectEntriesByIdsQuery(Connection connection, List<Long> ids) throws SQLException {
    String[] placeholderList = new String[ids.size()];
    Arrays.fill(placeholderList, "?");
    String placeholders = String.join(", ", placeholderList);
    String query = MessageFormat.format(selectEntriesByIds(), placeholders);
    PreparedStatement preparedStatement = connection.prepareStatement(query);

    for (int i = 0; i < ids.size(); i++) {
      preparedStatement.setLong(i + 1, ids.get(i));
    }

    return preparedStatement;
  }

  public PreparedStatement prepareSelectLastOffsetQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(selectLastOffsetQuery());
  }

  public PreparedStatement prepareSelectJournalCount(final Connection connection) throws SQLException {
    return connection.prepareStatement(selectJournalCountQuery());
  }

  public PreparedStatement prepareSelectSnapshotQuery(final Connection connection, final String streamName) throws SQLException {
    PreparedStatement selectSnapshot = connection.prepareStatement(selectSnapshotQuery());
    selectSnapshot.clearParameters();

    selectSnapshot.setString(1, streamName);

    return selectSnapshot;
  }

  public PreparedStatement prepareSelectStreamQuery(final Connection connection, final String streamName, final int streamVersion) throws SQLException {
    PreparedStatement selectStream = connection.prepareStatement(selectStreamQuery());
    selectStream.clearParameters();

    selectStream.setString(1, streamName);
    selectStream.setInt(2, streamVersion);

    return selectStream;
  }

  public PreparedStatement prepareUpdateOffsetQuery(final Connection connection, final String readerName, final long readerOffset) throws SQLException {
    PreparedStatement updateOffset = connection.prepareStatement(updateOffsetQuery());
    updateOffset.clearParameters();

    updateOffset.setLong(1, readerOffset);
    updateOffset.setString(2, readerName);

    return updateOffset;
  }

  public PreparedStatement prepareUpsertOffsetQuery(final Connection connection, final String readerName, final long readerOffset) throws SQLException {
    PreparedStatement upsertOffset = connection.prepareStatement(upsertOffsetQuery());
    upsertOffset.clearParameters();

    upsertOffset.setString(1, readerName);
    upsertOffset.setLong(2, readerOffset);
    upsertOffset.setLong(3, readerOffset);

    return upsertOffset;
  }

  private void close(final PreparedStatement statement) {
    try {
      statement.close();
    } catch (Exception e) {
      // ignore
    }
  }

  protected PreparedStatement newInsertEntryStatementQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(insertEntryQuery(), generatedKeysIndicator());
  }

  protected abstract String createDispatchableTable();

  protected abstract String createJournalTableQuery();

  protected abstract String createOffsetsTable();

  protected abstract String createSnapshotsTableQuery();

  protected abstract String deleteDispatchableQuery();

  protected abstract String dropDispatchablesTableQuery();

  protected abstract String dropJournalTable();

  protected abstract String dropOffsetsTable();

  protected abstract String dropSnapshotsTableQuery();

  protected abstract int generatedKeysIndicator();

  protected abstract String insertDispatchableQuery();

  protected abstract String insertEntryQuery();

  protected abstract String insertOffsetQuery();

  protected abstract String insertSnapshotQuery();

  protected abstract String selectCurrentOffset();

  protected abstract String selectDispatchablesQuery();

  protected abstract String selectEntryQuery();

  protected abstract String selectEntryBatchQuery();

  protected abstract String selectEntriesByIds();

  protected abstract String selectLastOffsetQuery();

  protected abstract String selectJournalCountQuery();

  protected abstract String selectSnapshotQuery();

  protected abstract String selectStreamQuery();

  protected abstract String updateOffsetQuery();

  protected abstract String upsertOffsetQuery();
}
