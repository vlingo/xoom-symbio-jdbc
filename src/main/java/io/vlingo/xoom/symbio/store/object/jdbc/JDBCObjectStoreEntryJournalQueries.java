// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;

/**
 * Query definitions for JDBC implementations.
 */
public abstract class JDBCObjectStoreEntryJournalQueries {
  public static final String DispatchablesTableName = "tbl_xoom_objectstore_dispatchables";
  public static final String EntryJournalTableName = "tbl_xoom_objectstore_entry_journal";
  public static final String EntryReaderOffsetsTableName = "tbl_xoom_objectstore_entryreader_offsets";

  public static final String ENTRY_DATATYPE_LONGVARCHAR = "LONGVARCHAR(65535)";
  public static final String ENTRY_DATATYPE_TEXT = "TEXT";

  private static final String QueryLastEntryId = "SELECT MAX(E_ID) FROM "; // append name
  private static final String QuerySize = "SELECT COUNT(*) FROM "; // append name

  private static final int CreatedOn = 0;
  private static final int OriginatorId = 1;
  private static final int Id = 2;
  private static final int StateId = 3;
  private static final int StateType = 4;
  private static final int StateTypeVersion = 5;
  private static final int StateData = 6;
  private static final int StateDataVersion = 7;
  private static final int StateMetadata = 8;
  private static final int Entries = 9;

  private static final int EntryType = 0;
  private static final int EntryTypeVersion = 1;
  private static final int EntryEntryData = 2;
  private static final int EntryMetadataValue = 3;
  private static final int EntryMetadataOperation = 4;
  private static final int EntryVersion = 5;

  /**
   * Answer a new {@code JDBCObjectStoreEntryJournalQueries} based on the {@code Configuration#databaseType}.
   * @param databaseType the DatabaseType
   * @return JDBCObjectStoreEntryJournalQueries
   */
  public static JDBCObjectStoreEntryJournalQueries using(final DatabaseType databaseType) {
    switch (databaseType) {
    case HSQLDB:
      return new HSQLDBObjectStoreEntryJournalQueries();
    case MySQL:
    case MariaDB:
      return  new MySQLObjectStoreEntryJournalQueries();
    case SQLServer:
      break;
    case Vitess:
      break;
    case Oracle:
      break;
    case Postgres:
      return new PostgresObjectStoreEntryJournalQueries();
    case YugaByte:
      return new YugaByteObjectStoreEntryJournalQueries();
    }

    throw new IllegalArgumentException("Database currently not supported: " + databaseType.name());
  }

  /**
   * Answer the parameterized query for retrieving a single {@code Entry} instance.
   * @return String
   */
  public String entryQuery() {
    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION  " +
            "FROM {0} WHERE E_ID >= ? ORDER BY E_ID LIMIT 1",
            EntryJournalTableName);
  }

  /**
   * Answer the query for retrieving a single {@code Entry} instance.
   * @param id the long identity to of the Entry
   * @return String
   */
  public String entryQuery(final long id) {
    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION " +
            "FROM {0} WHERE E_ID >= {1} ORDER BY E_ID LIMIT 1",
            EntryJournalTableName,
            id);
  }

  /**
   * Answer the parameterized query for retrieving multiple {@code Entry} instances.
   * @param placeholders the String[] of parameter placeholders
   * @return String
   */
  public String entriesQuery(final String[] placeholders) {
    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION " +
            "FROM {0} WHERE E_ID BETWEEN {1} AND {2} ORDER BY E_ID",
            EntryJournalTableName,
            placeholders[0],
            placeholders[1]);
  }

  /**
   * Answer the query for retrieving multiple {@code Entry} instances.
   * @param id the long identity to begin selection (possibly greater than this id)
   * @param count the int Entry instance limit
   * @return String
   */
  public String entriesQuery(final long id, final int count) {
    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION " +
            "FROM {0} WHERE E_ID BETWEEN {1} AND {2} ORDER BY E_ID",
            EntryJournalTableName,
            id,
            id + count - 1);
  }

  /**
   * Answer the query for retrieving multiple {@code Entry} instances based on ids.
   * @param ids List of ids
   * @return String
   */
  public String entriesQuery(List<Long> ids) {
    String[] arrayIds = ids.stream().map(id -> Long.toString(id)).toArray(String[]::new);
    StringBuilder placeholders = new StringBuilder("{0}");
    for (int i = 1; i < ids.size(); i++) {
      placeholders.append(", {" + i + "}");
    }

    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION " +
                    "FROM " + EntryJournalTableName + " WHERE E_ID IN (" + placeholders.toString() + ") ORDER BY E_ID",
            (Object[]) arrayIds);
  }

  /**
   * Answer the parameterized query for retrieving multiple {@code Entry} instances based on ids.
   * @param idsCount count of ids.
   * @return String
   */
  public String entriesQuery(final int idsCount) {
    StringBuilder inQuery = new StringBuilder("?");
    for (int i = 1; i < idsCount; i++) {
      inQuery.append(", ?");
    }

    return MessageFormat.format(
            "SELECT E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION " +
                    "FROM {0} WHERE E_ID IN ({1}) ORDER BY E_ID",
            EntryJournalTableName,
            inQuery.toString());
  }

  /**
   * Answer the query for deleting a given dispatchable.
   * @param idPlaceholder the String placeholder for the dispatchable id parameter
   * @return String
   */
  public String deleteDispatchableQuery(final String idPlaceholder) {
    return MessageFormat.format(
            "DELETE FROM {0} WHERE D_DISPATCH_ID = {1}",
            DispatchablesTableName,
            idPlaceholder);
  }

  /**
   * Answer the query for inserting a dispatchable.
   * @param placeholders the {@code String[]} placeholders for the dispatchable data parameters
   * @return String
   */
  public String insertDispatchableQuery(final String[] placeholders) {
    return MessageFormat.format(
           "INSERT INTO " + DispatchablesTableName
            + " (D_CREATED_AT, D_ORIGINATOR_ID, D_DISPATCH_ID, D_STATE_ID, D_STATE_TYPE,"
            + "  D_STATE_TYPE_VERSION, D_STATE_DATA, D_STATE_DATA_VERSION, D_STATE_METADATA, D_ENTRIES) "
            + " VALUES ( {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})",
            placeholders[CreatedOn], placeholders[OriginatorId], placeholders[Id], placeholders[StateId], placeholders[StateType],
            placeholders[StateTypeVersion], placeholders[StateData], placeholders[StateDataVersion], placeholders[StateMetadata], placeholders[Entries]);
  }

  /**
   * Answer the query for inserting an entry.
   * @param placeholders the {@code String[]} placeholders for the dispatchable data parameters
   * @return String
   */
  public String insertEntriesQuery(final String[] placeholders) {
    return MessageFormat.format(
            "INSERT INTO " + EntryJournalTableName
            + "(E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION) "
            + "VALUES ({0}, {1}, {2}, {3}, {4}, {5})",
            placeholders[EntryType], placeholders[EntryTypeVersion], placeholders[EntryEntryData],
            placeholders[EntryMetadataValue], placeholders[EntryMetadataOperation], placeholders[EntryVersion]);
  }

  /**
   * Answer the query for retrieving the last entry id.
   * @return String
   */
  public String lastEntryIdQuery() {
    return QueryLastEntryId + EntryJournalTableName;
  }

  /**
   * Answer the query for retrieving the size (number of entries) in the journal.
   * @return String
   */
  public String sizeQuery() {
    return QuerySize + EntryJournalTableName;
  }

  /**
   * Answer the query for retrieving all unconfirmed dispatchables.
   * @param originatorId the String indicating the identity of the original writer of the dispatchables to query
   * @return String
   */
  public String unconfirmedDispatchablesQuery(final String originatorId) {
    return MessageFormat.format(
            "SELECT * FROM {0} WHERE D_ORIGINATOR_ID = ''{1}'' ORDER BY D_CREATED_AT ASC",
            DispatchablesTableName,
            originatorId);
  }

  /**
   * Answer the query for inserting/updating the current entry offset.
   * @param placeholders the {@code String[]} placeholders for the dispatchable data parameters
   * @return String
   */
  public abstract String upsertCurrentEntryOffsetQuery(final String[] placeholders);

  /**
   * Answer the data type for wide text strings.
   * @return String
   */
  public abstract String wideTextDataType();

  /**
   * Create all common tables.
   *
   * @param connection
   * @throws SQLException when creation fails
   */
  public void createCommonTables(final Connection connection) throws SQLException {
    createTextEntryJournalTable(connection);
    createDispatchableTable(connection);
    createTextEntryJournalReaderOffsetsTable(connection);
  }

  /**
   * Creates the table to store {@code Dispatchable} objects.
   *
   * @param connection
   * @throws SQLException when creation fails
   */
  public void createDispatchableTable(final Connection connection) throws SQLException {
    final String wideTextDataType = wideTextDataType();

    connection
      .createStatement()
      .execute("CREATE TABLE IF NOT EXISTS " + DispatchablesTableName + " (\n" +
            "   D_ID BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY," +
            "   D_CREATED_AT TIMESTAMP NOT NULL," +
            "   D_ORIGINATOR_ID VARCHAR(32) NOT NULL," +
            "   D_DISPATCH_ID VARCHAR(128) NULL,\n" +
            "   D_STATE_ID VARCHAR(128) NULL, \n" +
            "   D_STATE_TYPE VARCHAR(256) NULL,\n" +
            "   D_STATE_TYPE_VERSION INT NULL,\n" +
            "   D_STATE_DATA " + wideTextDataType + " NULL,\n" +
            "   D_STATE_DATA_VERSION INT NULL,\n" +
            "   D_STATE_METADATA " + wideTextDataType + " NULL,\n" +
            "   D_ENTRIES " + wideTextDataType + " NULL \n" +
            ")" );

    connection
      .createStatement()
      .execute("CREATE INDEX IF NOT EXISTS IDX_DISPATCHABLES_DISPATCH_ID \n" +
            "ON " + DispatchablesTableName + " (D_DISPATCH_ID);");

    connection
      .createStatement()
      .execute("CREATE INDEX IF NOT EXISTS IDX_DISPATCHABLES_ORIGINATOR_ID \n" +
            "ON " + DispatchablesTableName + " (D_ORIGINATOR_ID);");
  }

  /**
   * Creates the table used to store journal {@code Entry} objects.
   *
   * @param connection
   * @throws SQLException when creation fails
   */
  public void createTextEntryJournalTable(final Connection connection) throws SQLException {
    connection
      .createStatement()
      .execute("CREATE TABLE IF NOT EXISTS " + EntryJournalTableName +
               " (E_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY, E_ENTRY_VERSION INTEGER NOT NULL, E_TYPE VARCHAR(1024), E_TYPE_VERSION INTEGER, E_DATA VARCHAR(8000), E_METADATA_VALUE VARCHAR(8000) NULL, E_METADATA_OP VARCHAR(128) NULL)");
  }

  /**
   * Creates the table used to store the current offsets of entry readers.
   *
   * @param connection
   * @throws SQLException when creation fails
   */
  public void createTextEntryJournalReaderOffsetsTable(final Connection connection) throws SQLException {
    connection
      .createStatement()
      .execute("CREATE TABLE IF NOT EXISTS " + EntryReaderOffsetsTableName +
               " (O_READER_NAME VARCHAR(1024) PRIMARY KEY, O_READER_OFFSET BIGINT NOT NULL)");
  }

  /**
   * Answer the {@code PreparedStatement} for retrieving the last entry id.
   *
   * @param connection
   * @return PreparedStatement
   * @throws SQLException when creation fails
   */
  public PreparedStatement statementForQueryLastEntryId(final Connection connection) throws SQLException {
    return connection.prepareStatement(lastEntryIdQuery());
  }

  /**
   * Answer the {@code PreparedStatement} for retrieving the size (number of entries) in the journal.
   *
   * @param connection
   * @return PreparedStatement
   * @throws SQLException when creation fails
   */
  public PreparedStatement statementForSizeQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(sizeQuery());
  }

  /**
   * Answer the parameterized {@code PreparedStatement} for retrieving a single {@code Entry} instance.
   *
   * @param connection
   * @return PreparedStatement
   * @throws SQLException when creation fails
   */
  public PreparedStatement statementForEntryQuery(final Connection connection) throws SQLException {
    return connection.prepareStatement(entryQuery());
  }

  /**
   * Answer the {@code PreparedStatement} for retrieving a single {@code Entry} instance.
   *
   * @param connection
   * @param id the long identity to select (possibly greater than this id)
   * @return PreparedStatement
   * @throws SQLException when creation fails
   */
  public PreparedStatement statementForEntryQuery(final Connection connection, final long id) throws SQLException {
    return connection.prepareStatement(entryQuery(id));
  }

  /**
   * Answer the parameterized {@code PreparedStatement} for retrieving multiple {@code Entry} instances.
   *
   * @param connection
   * @param placeholders the String[] of parameter placeholders
   * @return PreparedStatement
   * @throws SQLException  when creation fails
   */
  public PreparedStatement statementForEntriesQuery(final Connection connection, final String[] placeholders) throws SQLException {
    return connection.prepareStatement(entriesQuery(placeholders));
  }

  /**
   * Answer the parameterized {@code PreparedStatement} for retrieving multiple {@code Entry} instances based on ids.
   *
   * @param connection
   * @param idsCount the int number of Entry instances to find
   * @return PreparedStatement
   * @throws SQLException if the PreparedStatement creation fails
   */
  public PreparedStatement statementForEntriesQuery(final Connection connection, final int idsCount) throws SQLException {
    return connection.prepareStatement(entriesQuery(idsCount));
  }

  /**
   * Answer the {@code PreparedStatement} for retrieving multiple {@code Entry} instances.
   *
   * @param connection
   * @param id the long identity to begin selection (possibly greater than this id)
   * @param count the int Entry instance limit
   * @return PreparedStatement
   * @throws SQLException  when creation fails
   */
  public PreparedStatement statementForEntriesQuery(final Connection connection, final long id, final int count) throws SQLException {
    return connection.prepareStatement(entriesQuery(id, count));
  }

  /**
   * Answer the {@code PreparedStatement} for inserting/updating the current entry offset.
   *
   * @param connection
   * @param placeholders the {@code String[]} placeholders for the dispatchable data parameters
   * @return PreparedStatement
   * @throws SQLException  when creation fails
   */
  public PreparedStatement statementForUpsertCurrentEntryOffsetQuery(final Connection connection, final String[] placeholders) throws SQLException {
    return connection.prepareStatement(upsertCurrentEntryOffsetQuery(placeholders));
  }
}
