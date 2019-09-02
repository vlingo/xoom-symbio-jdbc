// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.object.ObjectStoreEntryReader;

/**
 * An {@code ObjectStoreEntryReader} for JDBC.
 */
public class JDBCObjectStoreEntryReaderActor extends Actor implements ObjectStoreEntryReader<Entry<String>> {

  private final Connection connection;
  private final JDBCObjectStoreEntryJournalQueries queries;
  private final String name;

  private final PreparedStatement entryQuery;
  private final PreparedStatement entriesQuery;

  private final PreparedStatement queryLastEntryId;
  private final PreparedStatement querySize;
  private final PreparedStatement upsertCurrentEntryOffset;

  private long offset;

  public JDBCObjectStoreEntryReaderActor(final DatabaseType databaseType, final Connection connection, final String name) throws SQLException {
    this.queries = JDBCObjectStoreEntryJournalQueries.using(databaseType, connection);
    this.name = name;
    this.connection = connection;
    this.offset = 1L;

    this.connection.setAutoCommit(true);

    this.entryQuery = queries.statementForEntryQuery();
    this.entriesQuery = queries.statementForEntriesQuery(new String[] { "?", "?" });

    this.queryLastEntryId = queries.statementForQueryLastEntryId();
    this.querySize = queries.statementForSizeQuery();
    this.upsertCurrentEntryOffset = queries.statementForUpsertCurrentEntryOffsetQuery(new String[] { "?", "?" });

    queries.createTextEntryJournalReaderOffsetsTable();

    restoreCurrentOffset();
  }

  @Override
  public void close() {
    try {
      if (!connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException e) {
      // ignore
    }
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<Entry<String>> readNext() {
    try {
      entryQuery.clearParameters();
      entryQuery.setLong(1, offset);
      try (final ResultSet result = entryQuery.executeQuery()) {
        final Entry<String> entry = mapQueriedEntryFrom(result);
        ++offset;
        updateCurrentOffset();
        return completes().with(entry);
      }
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<Entry<String>> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  public Completes<List<Entry<String>>> readNext(final int maximumEntries) {
    try {
      entriesQuery.clearParameters();
      entriesQuery.setLong(1, offset);
      entriesQuery.setLong(2, offset + maximumEntries - 1L);
      try (final ResultSet result = entriesQuery.executeQuery()) {
        final List<Entry<String>> entries = mapQueriedEntriesFrom(result);
        offset += entries.size();
        updateCurrentOffset();
        return completes().with(entries);
      }
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<List<Entry<String>>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {
    this.offset = 1L;
    updateCurrentOffset();
  }

  @Override
  public Completes<String> seekTo(final String id) {
    switch (id) {
    case Beginning:
        this.offset = 1L;
        updateCurrentOffset();
        break;
    case End:
        this.offset = retrieveLatestOffset() + 1L;
        updateCurrentOffset();
        break;
    case Query:
        break;
    default:
        this.offset = Long.parseLong(id);
        updateCurrentOffset();
        break;
    }

    return completes().with(String.valueOf(offset));
  }

  @Override
  public Completes<Long> size() {
    try (final ResultSet result = querySize.executeQuery()) {
      if (result.next()) {
        final long size = result.getLong(1);
        return completes().with(size);
      }
    } catch (Exception e) {
      // fall through
    }

    logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve size, using -1L.");

    return completes().with(-1L);
  }

  private List<Entry<String>> mapQueriedEntriesFrom(final ResultSet result) throws SQLException {
    final List<Entry<String>> entries = new ArrayList<>();
    while (result.next()) {
      final Entry<String> entry = mapEntryRowFrom(result);
      entries.add(entry);
    }
    return entries;
  }

  private Entry<String> mapQueriedEntryFrom(final ResultSet result) throws Exception {
    if (result.next()) {
      return mapEntryRowFrom(result);
    }
    return null;
  }

  private Entry<String> mapEntryRowFrom(final ResultSet result) throws SQLException {
    // E_ID,E_TYPE,E_TYPE_VERSION,E_DATA,E_METADATA_VALUE,E_METADATA_OP
    final String id = result.getString(1);
    final String entryType = result.getString(2);
    final int eventTypeVersion = result.getInt(3);
    final String entryData = result.getString(4);
    final String entryMetadata = result.getString(5);
    final String entryMetadataOp = result.getString(6);

    return new TextEntry(id, Entry.typed(entryType), eventTypeVersion, entryData, Metadata.with(entryMetadata, entryMetadataOp));
  }

  private void restoreCurrentOffset() {
    this.offset = retrieveLatestOffset();
  }

  private long retrieveLatestOffset() {
    try (final ResultSet result = queryLastEntryId.executeQuery()) {
      if (result.next()) {
        final long latestId = result.getLong(1);
        return latestId > 0 ? latestId : 1L;
      }
    } catch (Exception e) {
      // fall through
    }

    logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve latest offset, using current.");

    return offset;
  }

  private void updateCurrentOffset() {
    try {
      upsertCurrentEntryOffset.clearParameters();
      upsertCurrentEntryOffset.setString(1, name);
      upsertCurrentEntryOffset.setLong(2, offset);
      upsertCurrentEntryOffset.setLong(3, offset);
      upsertCurrentEntryOffset.executeUpdate();
    } catch (SQLException e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not upsert current offset because: " + e.getMessage(), e);
    }
  }
}
