// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.store.EntryReaderStream;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.journal.JournalReader;
import io.vlingo.xoom.symbio.store.object.ObjectStoreEntryReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An {@code ObjectStoreEntryReader} for JDBC.
 */
public class JDBCObjectStoreEntryReaderActor extends Actor implements ObjectStoreEntryReader<Entry<String>> {

  private final ConnectionProvider connectionProvider;
  private final EntryAdapterProvider entryAdapterProvider;
  private final JDBCObjectStoreEntryJournalQueries queries;
  private final String name;

  private GapRetryReader<Entry<String>> reader = null;

  private long offset;

  public JDBCObjectStoreEntryReaderActor(final DatabaseType databaseType, final ConnectionProvider connectionProvider, final String name) throws SQLException {
    this.name = name;
    this.connectionProvider = connectionProvider;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.offset = 1L;

    // this.connection.setAutoCommit(true);

    try (Connection initConnection = connectionProvider.connection()) {
      this.queries = JDBCObjectStoreEntryJournalQueries.using(databaseType);
      queries.createTextEntryJournalReaderOffsetsTable(initConnection);
      restoreCurrentOffset(initConnection);
    }
  }

  private GapRetryReader<Entry<String>> reader() {
    if (reader == null) {
      reader = new GapRetryReader<>(stage(), scheduler());
    }

    return reader;
  }

  @Override
  public void close() {
    // no resources to be closed
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<Entry<String>> readNext() {
    try (final Connection connection = connectionProvider.connection();
         final PreparedStatement entryQuery = queries.statementForEntriesQuery(connection, new String[]{"?", "?"})) {
      entryQuery.clearParameters();
      entryQuery.setLong(1, offset);
      try (final ResultSet result = entryQuery.executeQuery()) {
        final Entry<String> entry = mapQueriedEntryFrom(result);
        List<Long> gapIds = reader().detectGaps(entry, offset);
        if (!gapIds.isEmpty()) {
          // gaps have been detected
          List<Entry<String>> entries = entry == null ? new ArrayList<>() : Collections.singletonList(entry);
          GappedEntries<Entry<String>> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          ++offset;
          updateCurrentOffset(connection);
          return completes();
        } else {
          ++offset;
          updateCurrentOffset(connection);
          return completes().with(entry);
        }
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
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
    try (final Connection connection = connectionProvider.connection();
         final PreparedStatement entriesQuery = queries.statementForEntriesQuery(connection, new String[]{"?", "?"})) {
      entriesQuery.clearParameters();
      entriesQuery.setLong(1, offset);
      entriesQuery.setLong(2, offset + maximumEntries - 1L);
      try (final ResultSet result = entriesQuery.executeQuery()) {
        final List<Entry<String>> entries = mapQueriedEntriesFrom(result);
        List<Long> gapIds = reader().detectGaps(entries, offset, maximumEntries);
        if (!gapIds.isEmpty()) {
          GappedEntries<Entry<String>> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          // Move offset with maximumEntries regardless of filled up gaps
          offset += maximumEntries;
          updateCurrentOffset(connection);
          return completes();
        } else {
          offset += maximumEntries;
          updateCurrentOffset(connection);
          return completes().with(entries);
        }
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
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

    try (final Connection connection = connectionProvider.connection()) {
      updateCurrentOffset(connection);
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not rewind because: " + e.getMessage(), e);
    }
  }

  @Override
  public Completes<String> seekTo(final String id) {
    try (final Connection connection = connectionProvider.connection()) {
      switch (id) {
        case Beginning:
          this.offset = 1L;
          updateCurrentOffset(connection);
          break;
        case End:
          this.offset = retrieveLatestOffset(connection) + 1L;
          updateCurrentOffset(connection);
          break;
        case Query:
          break;
        default:
          this.offset = Long.parseLong(id);
          updateCurrentOffset(connection);
          break;
      }

      return completes().with(String.valueOf(offset));
    } catch (SQLException e) {

      return completes().with(null);
    }
  }

  @Override
  public Completes<Long> size() {
    try (final Connection connection = connectionProvider.connection();
         final PreparedStatement querySize = queries.statementForSizeQuery(connection);
         final ResultSet result = querySize.executeQuery()) {
      if (result.next()) {
        final long size = result.getLong(1);
        return completes().with(size);
      }
    } catch (Exception e) {
      // fall through
    }

    logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve size, using -1L.");

    return completes().with(-1L);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<Stream> streamAll() {
    return completes().with(new EntryReaderStream<>(stage(), selfAs(JournalReader.class), entryAdapterProvider));
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
    // E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP, E_ENTRY_VERSION
    final String id = result.getString(1);
    final String entryType = result.getString(2);
    final int eventTypeVersion = result.getInt(3);
    final String entryData = result.getString(4);
    final String entryMetadata = result.getString(5);
    final String entryMetadataOp = result.getString(6);
    final int entryVersion = result.getInt(7); // from E_ENTRY_VERSION

    return new TextEntry(id, Entry.typed(entryType), eventTypeVersion, entryData, entryVersion, Metadata.with(entryMetadata, entryMetadataOp));
  }

  private PreparedStatement prepareQueryByIdsStatement(final Connection connection, final List<Long> ids) throws SQLException {
    PreparedStatement statement = queries.statementForEntriesQuery(connection, ids.size());
    for (int i = 0; i < ids.size(); i++) {
      // parameter index starts from 1
      statement.setLong(i + 1, ids.get(i));
    }

    return statement;
  }

  private List<Entry<String>> readIds(List<Long> ids) {
    try (final Connection connection = connectionProvider.connection();
         final PreparedStatement statement = prepareQueryByIdsStatement(connection, ids);
         final ResultSet result = statement.executeQuery()) {
      return mapQueriedEntriesFrom(result);
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read ids because: " + e.getMessage(), e);
      return new ArrayList<>();
    }
  }

  private void restoreCurrentOffset(final Connection connection) {
    this.offset = retrieveLatestOffset(connection);
  }

  private long retrieveLatestOffset(final Connection connection) {
    try (final PreparedStatement queryLastEntryId = queries.statementForQueryLastEntryId(connection);
         final ResultSet result = queryLastEntryId.executeQuery()) {
      if (result.next()) {
        final long latestId = result.getLong(1);
        return latestId > 0 ? latestId : 1L;
      }
    } catch (Exception e) {
      // fall through
    }

    logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve latest offset, using current.");

    return offset;
  }

  private void updateCurrentOffset(final Connection connection) {
    try (final PreparedStatement upsertCurrentEntryOffset = queries.statementForUpsertCurrentEntryOffsetQuery(connection, new String[]{"?", "?"})) {
      upsertCurrentEntryOffset.clearParameters();
      upsertCurrentEntryOffset.setString(1, name);
      upsertCurrentEntryOffset.setLong(2, offset);
      upsertCurrentEntryOffset.setLong(3, offset);
      upsertCurrentEntryOffset.executeUpdate();
    } catch (SQLException e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not upsert current offset because: " + e.getMessage(), e);
    }
  }

  public static class JDBCObjectStoreEntryReaderInstantiator implements ActorInstantiator<JDBCObjectStoreEntryReaderActor> {
    private static final long serialVersionUID = 4022321764623417613L;

    private final ConnectionProvider connectionProvider;
    private final DatabaseType databaseType;
    private final String name;

    public JDBCObjectStoreEntryReaderInstantiator(final DatabaseType databaseType, final ConnectionProvider connectionProvider, final String name) {
      this.databaseType = databaseType;
      this.connectionProvider = connectionProvider;
      this.name = name;
    }

    @Override
    public JDBCObjectStoreEntryReaderActor instantiate() {
      try {
        return new JDBCObjectStoreEntryReaderActor(databaseType, connectionProvider, name);
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
