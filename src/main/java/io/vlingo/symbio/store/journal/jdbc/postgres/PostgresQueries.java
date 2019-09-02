// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import io.vlingo.common.Tuple2;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.journal.jdbc.postgres.yugabyte.YugaByteQueries;

/**
 * Standard queries for the Postgres `Journal` and may be extended
 * by others implementations, such as YugaByte.
 */
public class PostgresQueries {
  public static final String TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES = "VLINGO_SYMBIO_JOURNAL_DISPATCHABLES";
  public static final String TABLE_VLINGO_SYMBIO_JOURNAL = "VLINGO_SYMBIO_JOURNAL";
  public static final String TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS = "VLINGO_SYMBIO_JOURNAL_OFFSETS";
  public static final String TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS = "VLINGO_SYMBIO_JOURNAL_SNAPSHOTS";

  private static final String CREATE_DISPATCHABLE_TABLE =
          "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " (\n" +
                  "   D_DISPATCH_ID VARCHAR(512) PRIMARY KEY,\n" +
                  "   D_ORIGINATOR_ID VARCHAR(512) NOT NULL," +
                  "   D_CREATED_ON BIGINT NOT NULL," +
                  "   D_STATE_ID VARCHAR(512) NULL, \n" +
                  "   D_STATE_DATA TEXT NULL,\n" +
                  "   D_STATE_DATA_VERSION INT NULL,\n" +
                  "   D_STATE_TYPE VARCHAR(512) NULL,\n" +
                  "   D_STATE_TYPE_VERSION INTEGER NULL,\n" +
                  "   D_STATE_METADATA TEXT NULL,\n" +
                  "   D_ENTRIES TEXT NOT NULL\n" +
                  ");";

  private static final String CREATE_JOURNAL_TABLE =
          "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL + " (\n" +
//                  "E_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY, \n" +
                  "E_ID BIGSERIAL PRIMARY KEY, \n" +
                  "E_STREAM_NAME VARCHAR(512) NOT NULL, \n" +
                  "E_STREAM_VERSION INTEGER NOT NULL, \n" +
                  "E_ENTRY_DATA TEXT NOT NULL, \n" +
                  "E_ENTRY_TYPE VARCHAR(512) NOT NULL, \n" +
                  "E_ENTRY_TYPE_VERSION INTEGER NOT NULL, \n" +
                  "E_ENTRY_METADATA TEXT NOT NULL \n" +
                  ")";

  private static final String CREATE_OFFSETS_TABLE =
          "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + "(" +
                  "O_READER_NAME VARCHAR(128) PRIMARY KEY," +
                  "O_READER_OFFSET BIGINT NOT NULL" +
                  ")";

  private static final String CREATE_SNAPSHOTS_TABLE =
          "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + " (\n" +
                  "S_STREAM_NAME VARCHAR(512) NOT NULL, \n" +
                  "S_STREAM_VERSION INTEGER NOT NULL, \n" +
                  "S_SNAPSHOT_DATA TEXT NOT NULL, \n" +
                  "S_SNAPSHOT_DATA_VERSION INTEGER NOT NULL, \n" +
                  "S_SNAPSHOT_TYPE VARCHAR(512) NOT NULL, \n" +
                  "S_SNAPSHOT_TYPE_VERSION INTEGER NOT NULL, \n" +
                  "S_SNAPSHOT_METADATA TEXT NOT NULL, \n\n" +

                  "PRIMARY KEY (S_STREAM_NAME, S_STREAM_VERSION) \n" +
                  ")";

  private final static String DELETE_DISPATCHABLE =
          "DELETE FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " " +
          "WHERE D_DISPATCH_ID = ?";

  private static final String DROP_DISPATCHABLES_TABLE =
          "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES;

  private static final String DROP_JOURNAL_TABLE =
          "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL;

  private static final String DROP_OFFSETS_TABLE =
          "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS;

  private static final String DROP_SNAPSHOTS_TABLE =
          "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS;

  private final static String INSERT_DISPATCHABLE =
          "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " \n" +
                  "(D_DISPATCH_ID, D_ORIGINATOR_ID, D_CREATED_ON, \n" +
                  " D_STATE_ID, D_STATE_DATA, D_STATE_DATA_VERSION, \n" +
                  " D_STATE_TYPE, D_STATE_TYPE_VERSION, \n" +
                  " D_STATE_METADATA, D_ENTRIES) \n" +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String INSERT_ENTRY =
          "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL + " \n" +
                  "(E_STREAM_NAME, E_STREAM_VERSION, E_ENTRY_DATA, \n" +
                  " E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA) \n" +
          "VALUES(?, ?, ?, ?, ?, ?)";

  private static final String INSERT_OFFSET =
          "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " (O_READER_NAME, O_READER_OFFSET) VALUES(?, ?)";

  private static final String UPDATE_OFFSET =
          "UPDATE  " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " SET O_READER_OFFSET = ? WHERE O_READER_NAME = ?";

  private static final String UPSERT_OFFSET =
          "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + "(O_READER_NAME, O_READER_OFFSET) VALUES(?, ?) " +
                  "ON CONFLICT (O_READER_NAME) DO UPDATE SET O_READER_OFFSET=?";

  private static final String INSERT_SNAPSHOT =
          "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + "\n" +
                  "(S_STREAM_NAME, S_STREAM_VERSION, \n" +
                  " S_SNAPSHOT_DATA, S_SNAPSHOT_DATA_VERSION, \n" +
                  " S_SNAPSHOT_TYPE, S_SNAPSHOT_TYPE_VERSION, \n" +
                  " S_SNAPSHOT_METADATA) \n" +
          "VALUES(?, ?, ?, ?, ?, ?, ?)";

  private static final String SELECT_CURRENT_OFFSET =
          "SELECT O_READER_OFFSET FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " WHERE O_READER_NAME=?";

  private final static String SELECT_DISPATCHABLES =
          "SELECT D_DISPATCH_ID, D_CREATED_ON, \n" +
                " D_STATE_ID, D_STATE_DATA, D_STATE_DATA_VERSION, \n" +
                " D_STATE_TYPE, D_STATE_TYPE_VERSION, \n" +
                " D_STATE_METADATA, D_ENTRIES \n" +
          " FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + "\n" +
          " WHERE D_ORIGINATOR_ID = ? ORDER BY D_CREATED_ON";

  private static final String SELECT_ENTRY =
          "SELECT E_ID, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA " +
          "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
          "WHERE E_ID = ?";

  private static final String SELECT_ENTRY_BATCH =
          "SELECT E_ID, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA " +
          "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
          "WHERE E_ID BETWEEN ? AND ? ORDER BY E_ID";

  private static final String SELECT_LAST_OFFSET =
          "SELECT MAX(E_ID) FROM " + TABLE_VLINGO_SYMBIO_JOURNAL;

  private static final String SELECT_JOURNAL_COUNT =
          "SELECT COUNT(*) FROM " + TABLE_VLINGO_SYMBIO_JOURNAL;

  private static final String SELECT_SNAPSHOT =
          "SELECT S_SNAPSHOT_DATA, S_SNAPSHOT_DATA_VERSION, S_SNAPSHOT_TYPE, S_SNAPSHOT_TYPE_VERSION, S_SNAPSHOT_METADATA " +
          "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + " WHERE S_STREAM_NAME = ?";

  private static final String SELECT_STREAM =
          "SELECT E_ID, E_STREAM_VERSION, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA " +
          "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
          "WHERE E_STREAM_NAME = ? AND E_STREAM_VERSION >= ? ORDER BY E_STREAM_VERSION";

  protected final Connection connection;

  protected final PreparedStatement deleteDispatchable;

  protected final PreparedStatement insertEntry;
  protected final PreparedStatement insertOffset;
  protected final PreparedStatement insertSnapshot;
  protected final PreparedStatement insertDispatchable;

  protected final PreparedStatement selectCurrentOffset;
  protected final PreparedStatement selectDispatchables;
  protected final PreparedStatement selectLastOffset;
  protected final PreparedStatement selectJournalCount;
  protected final PreparedStatement selectEntry;
  protected final PreparedStatement selectEntryBatch;
  protected final PreparedStatement selectSnapshot;
  protected final PreparedStatement selectStream;

  protected final PreparedStatement updateOffset;
  protected final PreparedStatement upsertOffset;

  /**
   * Answer a new {@code PostgresQueries} per the {@code DatabaseType} of the {@code connection}.
   * @param connection the Connection to use
   * @return PostgresQueries
   * @throws SQLException if the specific PostgresQueries cannot be created
   */
  public static PostgresQueries queriesFor(final Connection connection) throws SQLException {
    final DatabaseType databaseType = DatabaseType.databaseType(connection);

    switch (databaseType) {
    case Postgres:
      return new PostgresQueries(connection);
    case YugaByte:
      return new YugaByteQueries(connection);
    default:
      throw new IllegalArgumentException("Database type not supported: " + databaseType);
    }
  }

  public PostgresQueries(final Connection connection) throws SQLException {
    this.connection = connection;

    this.deleteDispatchable = connection.prepareStatement(deleteDispatchableQuery());

    this.insertEntry = connection.prepareStatement(insertEntryQuery(), generatedKeysIndicator());
    this.insertOffset = connection.prepareStatement(insertOffsetQuery());
    this.insertSnapshot = connection.prepareStatement(insertSnapshotQuery());
    this.insertDispatchable = connection.prepareStatement(insertDispatchableQuery());

    this.selectCurrentOffset = connection.prepareStatement(selectCurrentOffset());
    this.selectDispatchables = connection.prepareStatement(selectDispatchablesQuery());
    this.selectEntry = connection.prepareStatement(selectEntryQuery());
    this.selectEntryBatch = connection.prepareStatement(selectEntryBatchQuery());
    this.selectLastOffset = connection.prepareStatement(selectLastOffsetQuery());
    this.selectJournalCount = connection.prepareStatement(selectJournalCountQuery());
    this.selectSnapshot = connection.prepareStatement(selectSnapshotQuery());
    this.selectStream = connection.prepareStatement(selectStreamQuery());

    this.updateOffset = connection.prepareStatement(updateOffsetQuery());
    this.upsertOffset = connection.prepareStatement(upsertOffsetQuery());
  }

  public void close() throws SQLException {
    close(deleteDispatchable);
    close(insertEntry);
    close(insertOffset);
    close(insertSnapshot);
    close(insertDispatchable);
    close(selectCurrentOffset);
    close(selectDispatchables);
    close(selectEntry);
    close(selectEntryBatch);
    close(selectLastOffset);
    close(selectJournalCount);
    close(selectSnapshot);
    close(selectStream);
    close(updateOffset);
    close(upsertOffset);

    connection.close();
  }

  public void createTables() throws SQLException {
    connection.createStatement().execute(createJournalTableQuery());
    connection.commit();
    connection.createStatement().execute(createOffsetsTable());
    connection.commit();
    connection.createStatement().execute(createSnapshotsTableQuery());
    connection.commit();
    connection.createStatement().execute(createDispatchableTable());
    connection.commit();
  }

  public void dropTables() throws SQLException {
    connection.prepareStatement(dropDispatchablesTableQuery()).execute();
    connection.commit();
    connection.prepareStatement(dropSnapshotsTableQuery()).execute();
    connection.commit();
    connection.prepareStatement(dropOffsetsTable()).execute();
    connection.commit();
    connection.prepareStatement(dropJournalTable()).execute();
    connection.commit();
  }

  public PreparedStatement prepareDeleteDispatchableQuery(
          final String dispatchableId)
  throws SQLException {

    deleteDispatchable.clearParameters();

    deleteDispatchable.setString(1, dispatchableId);

    return deleteDispatchable;
  }

  public long generatedKeyFrom(PreparedStatement insertStatement) throws SQLException {
    final ResultSet result = insertStatement.getGeneratedKeys();
    if (result.next()) {
      return result.getLong(1);
    }
    return -1L;
  }

  public Tuple2<PreparedStatement,Optional<String>> prepareInsertDispatchableQuery(
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
    insertDispatchable.setLong(3, System.nanoTime());

    insertDispatchable.setString(4, d_state_id);
    insertDispatchable.setString(5, d_state_data);
    insertDispatchable.setInt(6, d_state_data_version);
    insertDispatchable.setString(7, d_state_type);
    insertDispatchable.setInt(8, d_state_type_version);
    insertDispatchable.setString(9, d_state_metadata);
    insertDispatchable.setString(10, d_entries);

    return Tuple2.from(insertDispatchable, Optional.empty());
  }

  public Tuple2<PreparedStatement,Optional<String>> prepareInsertEntryQuery(
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

    return Tuple2.from(insertEntry, Optional.empty());
  }

  public PreparedStatement prepareInsertOffsetQuery(
          final String readerName,
          final long readerOffset)
  throws SQLException {

    insertOffset.clearParameters();

    insertOffset.setString(1, readerName);
    insertOffset.setLong(2, readerOffset);

    return insertOffset;
  }

  public Tuple2<PreparedStatement,Optional<String>> prepareInsertSnapshotQuery(
          final String stream_name,
          final int stream_version,
          final String e_snapshot_data,
          final int e_snapshot_data_version,
          final String e_snapshot_type,
          final int e_snapshot_type_version,
          final String e_snapshot_metadata)
  throws SQLException {

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

  public PreparedStatement prepareSelectCurrentOffsetQuery(
          final String readerName)
  throws SQLException {

    selectCurrentOffset.clearParameters();

    selectCurrentOffset.setString(1, readerName);

    return selectCurrentOffset;
  }

  public PreparedStatement prepareSelectDispatchablesQuery(
          final String oringinatorId)
  throws SQLException {

    selectDispatchables.clearParameters();

    selectDispatchables.setString(1, oringinatorId);

    return selectDispatchables;
  }

  public PreparedStatement prepareSelectEntryQuery(
          final long entryId)
  throws SQLException {

    selectEntry.clearParameters();

    selectEntry.setLong(1, entryId);

    return selectEntry;
  }

  public PreparedStatement prepareSelectEntryBatchQuery(
          final long entryId,
          final int count)
  throws SQLException {

    selectEntryBatch.clearParameters();

    selectEntryBatch.setLong(1, entryId);
    selectEntryBatch.setLong(2, entryId + count - 1);

    return selectEntryBatch;
  }

  public PreparedStatement prepareSelectLastOffsetQuery() {
    return selectLastOffset;
  }

  public PreparedStatement prepareSelectJournalCount() {
    return selectJournalCount;
  }

  public PreparedStatement prepareSelectSnapshotQuery(
          final String streamName)
  throws SQLException {

    selectSnapshot.clearParameters();

    selectSnapshot.setString(1, streamName);

    return selectSnapshot;
  }

  public PreparedStatement prepareSelectStreamQuery(
          final String streamName,
          final int streamVersion)
  throws SQLException {

    selectStream.clearParameters();

    selectStream.setString(1, streamName);
    selectStream.setInt(2, streamVersion);

    return selectStream;
  }

  public PreparedStatement prepareUpdateOffsetQuery(
          final String readerName,
          final long readerOffset)
  throws SQLException {

    updateOffset.clearParameters();

    updateOffset.setLong(1, readerOffset);
    updateOffset.setString(2, readerName);

    return updateOffset;
  }

  public PreparedStatement prepareUpsertOffsetQuery(
          final String readerName,
          final long readerOffset)
  throws SQLException {

    upsertOffset.clearParameters();

    upsertOffset.setString(1, readerName);
    upsertOffset.setLong(2, readerOffset);
    upsertOffset.setLong(3, readerOffset);

    return upsertOffset;
  }

  protected String createDispatchableTable() {
    return CREATE_DISPATCHABLE_TABLE;
  }

  protected String createJournalTableQuery() {
    return CREATE_JOURNAL_TABLE;
  }

  protected String createOffsetsTable() {
    return CREATE_OFFSETS_TABLE;
  }

  protected String createSnapshotsTableQuery() {
    return CREATE_SNAPSHOTS_TABLE;
  }

  protected String deleteDispatchableQuery() {
    return DELETE_DISPATCHABLE;
  }

  protected String dropDispatchablesTableQuery() {
    return DROP_DISPATCHABLES_TABLE;
  }

  protected String dropJournalTable() {
    return DROP_JOURNAL_TABLE;
  }

  protected String dropOffsetsTable() {
    return DROP_OFFSETS_TABLE;
  }

  protected String dropSnapshotsTableQuery() {
    return DROP_SNAPSHOTS_TABLE;
  }

  protected int generatedKeysIndicator() {
    return Statement.RETURN_GENERATED_KEYS;
  }

  protected String insertDispatchableQuery() {
    return INSERT_DISPATCHABLE;
  }

  protected String insertEntryQuery() {
    return INSERT_ENTRY;
  }

  protected String insertOffsetQuery() {
    return INSERT_OFFSET;
  }

  protected String insertSnapshotQuery() {
    return INSERT_SNAPSHOT;
  }

  protected String selectCurrentOffset() {
    return SELECT_CURRENT_OFFSET;
  }

  protected String selectDispatchablesQuery() {
    return SELECT_DISPATCHABLES;
  }

  protected String selectEntryQuery() {
    return SELECT_ENTRY;
  }

  protected String selectEntryBatchQuery() {
    return SELECT_ENTRY_BATCH;
  }

  protected String selectLastOffsetQuery() {
    return SELECT_LAST_OFFSET;
  }

  protected String selectJournalCountQuery() {
    return SELECT_JOURNAL_COUNT;
  }

  protected String selectSnapshotQuery() {
    return SELECT_SNAPSHOT;
  }

  protected String selectStreamQuery() {
    return SELECT_STREAM;
  }

  protected String updateOffsetQuery() {
    return UPDATE_OFFSET;
  }

  protected String upsertOffsetQuery() {
    return UPSERT_OFFSET;
  }

  private void close(final PreparedStatement statement) {
    try {
      statement.close();
    } catch (Exception e) {
      // ignore
    }
  }
}
