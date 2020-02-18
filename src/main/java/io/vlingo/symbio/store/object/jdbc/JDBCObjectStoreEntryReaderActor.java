// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
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
import java.util.Collections;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.gap.GapRetryReader;
import io.vlingo.symbio.store.gap.GappedEntries;
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

  private GapRetryReader<String> reader = null;

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

  private GapRetryReader<String> reader() {
    if (reader == null) {
      reader = new GapRetryReader<>(stage(), scheduler());
    }

    return reader;
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

  private List<Entry<String>> readIds(List<Long> ids) {
    try {
      PreparedStatement statement = queries.statementForEntriesQuery(ids.size());
      for (int i = 0; i < ids.size(); i++) {
        // parameter index starts from 1
        statement.setLong(i + 1, ids.get(i));
      }

      try (final ResultSet result = statement.executeQuery()) {
        return mapQueriedEntriesFrom(result);
      }
    } catch (Exception e) {
      logger().info("vlingo/symbio-jdbc: " + getClass().getSimpleName() + " Could not read ids because: " + e.getMessage(), e);
      return new ArrayList<>();
    }
  }

  @Override
  public Completes<Entry<String>> readNext() {
    try {
      entryQuery.clearParameters();
      entryQuery.setLong(1, offset);
      try (final ResultSet result = entryQuery.executeQuery()) {
        final Entry<String> entry = mapQueriedEntryFrom(result);
        List<Long> gapIds = reader().detectGaps(entry, offset);
        if (!gapIds.isEmpty()) {
          // gaps have been detected
          List<Entry<String>> entries = entry == null ? new ArrayList<>() : Collections.singletonList(entry);
          GappedEntries<String> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          ++offset;
          updateCurrentOffset();
          return completes();
        } else {
          ++offset;
          updateCurrentOffset();
          return completes().with(entry);
        }
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
        List<Long> gapIds = reader().detectGaps(entries, offset, maximumEntries);
        if (!gapIds.isEmpty()) {
          GappedEntries<String> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          // Move offset with maximumEntries regardless of filled up gaps
          offset += maximumEntries;
          updateCurrentOffset();
          return completes();
        } else {
          offset += maximumEntries;
          updateCurrentOffset();
          return completes().with(entries);
        }
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

  public static class JDBCObjectStoreEntryReaderInstantiator implements ActorInstantiator<JDBCObjectStoreEntryReaderActor> {
    private static final long serialVersionUID = 4022321764623417613L;

    private final Connection connection;
    private final DatabaseType databaseType;
    private final String name;

    public JDBCObjectStoreEntryReaderInstantiator(final DatabaseType databaseType, final Connection connection, final String name) {
      this.databaseType = databaseType;
      this.connection = connection;
      this.name = name;
    }

    @Override
    public JDBCObjectStoreEntryReaderActor instantiate() {
      try {
        return new JDBCObjectStoreEntryReaderActor(databaseType, connection, name);
      } catch (SQLException e) {
        throw new IllegalArgumentException("Failed instantiator of " + getClass() + " because: " + e.getMessage(), e);
      }
    }

    @Override
    public Class<JDBCObjectStoreEntryReaderActor> type() {
      return JDBCObjectStoreEntryReaderActor.class;
    }
  }
}
