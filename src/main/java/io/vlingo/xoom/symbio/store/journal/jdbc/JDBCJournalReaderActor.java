// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.store.EntryReaderStream;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.journal.JournalReader;

public class JDBCJournalReaderActor extends Actor implements JournalReader<TextEntry> {
  private final ConnectionProvider connectionProvider;
  private final EntryAdapterProvider entryAdapterProvider;
  private final DatabaseType databaseType;
  private final Gson gson;
  private final String name;
  private final JDBCQueries queries;

  private GapRetryReader<TextEntry> reader = null;

  private long offset;

  public JDBCJournalReaderActor(final Configuration configuration, final String name) throws SQLException {
    this.connectionProvider = configuration.connectionProvider;
    this.databaseType = configuration.databaseType;
    this.name = name;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.gson = new Gson();

    try (Connection initConnection = connectionProvider.newConnection()) {
      try {
        this.queries = JDBCQueries.queriesFor(initConnection);
        retrieveCurrentOffset(initConnection);
        initConnection.commit();
      } catch (Exception e) {
        initConnection.rollback();
        throw new IllegalStateException("Failed to initialize JDBCJournalReaderActor because: " + e.getMessage(), e);
      }
    }
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
  public Completes<TextEntry> readNext() {
    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement selectEntry = queries.prepareSelectEntryQuery(connection, offset);
           final ResultSet resultSet = selectEntry.executeQuery()) {
        if (resultSet.next()) {
          TextEntry entry = entryFromResultSet(resultSet);

          ++offset;
          updateCurrentOffset(connection);
          connection.commit();
          return completes().with(entry);
        } else {
          List<Long> gapIds = reader().detectGaps(null, offset);
          GappedEntries<TextEntry> gappedEntries = new GappedEntries<>(new ArrayList<>(), gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          ++offset;
          updateCurrentOffset(connection);
          connection.commit();
          return completes();
        }
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readNext() error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readNext() connection error: " + e.getMessage(), e);
    }

    return completes().with(null);
  }

  @Override
  public Completes<TextEntry> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  public Completes<List<TextEntry>> readNext(final int maximumEntries) {
    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement selectQueryBatch = queries.prepareSelectEntryBatchQuery(connection, offset, maximumEntries);
           final ResultSet resultSet = selectQueryBatch.executeQuery()) {
        final List<TextEntry> entries = entriesFromResultSet(resultSet);
        List<Long> gapIds = reader().detectGaps(entries, offset, maximumEntries);
        if (!gapIds.isEmpty()) {
          GappedEntries<TextEntry> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
          reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

          // Move offset with maximumEntries regardless of filled up gaps
          offset += maximumEntries;
          updateCurrentOffset(connection);
          connection.commit();
          return completes();
        } else {
          offset += maximumEntries;
          updateCurrentOffset(connection);
          connection.commit();
          return completes().with(entries);
        }
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readNext(int) error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readNext(int) connection error: " + e.getMessage(), e);
    }

    return completes().with(null);
  }

  @Override
  public Completes<List<TextEntry>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {
    this.offset = 1;
    try (Connection connection = connectionProvider.newConnection()) {
      try {
        updateCurrentOffset(connection);
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " rewind(String, int): " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " rewind(String, int) connection error: " + e.getMessage(), e);
    }
  }

  @Override
  public Completes<String> seekTo(final String id) {
    try (Connection connection = connectionProvider.newConnection()) {
      try {
        switch (id) {
          case Beginning:
            this.offset = 1;
            updateCurrentOffset(connection);
            break;
          case End:
            this.offset = retrieveLastOffset(connection) + 1;
            updateCurrentOffset(connection);
            break;
          case Query:
            break;
          default:
            this.offset = Integer.parseInt(id);
            updateCurrentOffset(connection);
            break;
        }

        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " seekTo(String) error: " + e.getMessage(), e);
      }
    } catch (SQLException e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " seekTo(String) connection error: " + e.getMessage(), e);
    }

    return completes().with(String.valueOf(offset));
  }

  @Override
  public Completes<Long> size() {
    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement selectJournalCount = queries.prepareSelectJournalCount(connection);
           final ResultSet resultSet = selectJournalCount.executeQuery()) {
        if (resultSet.next()) {
          final long count = resultSet.getLong(1);
          connection.commit();
          return completes().with(count);
        }

        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " size() error: " + e.getMessage(), e);
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " size() connection error: " + e.getMessage(), e);
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
    }

    return completes().with(-1L);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<Stream> streamAll() {
    return completes().with(new EntryReaderStream<>(stage(), selfAs(JournalReader.class), entryAdapterProvider));
  }

  private TextEntry entryFromResultSet(final ResultSet resultSet) throws SQLException, ClassNotFoundException {
    final long id = resultSet.getLong(1);
    final String entryData = resultSet.getString(2);
    final String entryType = resultSet.getString(3);
    final int eventTypeVersion = resultSet.getInt(4);
    final String entryMetadata = resultSet.getString(5);
    final int entryVersion = resultSet.getInt(6); // from E_STREAM_VERSION

    final Class<?> classOfEvent = StoredTypes.forName(entryType);
    final Metadata eventMetadataDeserialized = gson.fromJson(entryMetadata, Metadata.class);

    return new BaseEntry.TextEntry(String.valueOf(id), classOfEvent, eventTypeVersion, entryData, entryVersion, eventMetadataDeserialized);
  }

  private List<TextEntry> entriesFromResultSet(ResultSet resultSet) throws SQLException, ClassNotFoundException {
    final List<TextEntry> entries = new ArrayList<>();
    while (resultSet.next()) {
      TextEntry entry = entryFromResultSet(resultSet);
      entries.add(entry);
    }

    return entries;
  }

  private GapRetryReader<TextEntry> reader() {
    if (reader == null) {
      reader = new GapRetryReader<>(stage(), scheduler());
    }

    return reader;
  }

  private List<TextEntry> readIds(final List<Long> ids) {
    try (final Connection connection = connectionProvider.newConnection()) {
      try (final PreparedStatement statement = queries.prepareNewSelectEntriesByIdsQuery(connection, ids);
           final ResultSet resultSet = statement.executeQuery()) {
        final List<TextEntry> entries = entriesFromResultSet(resultSet);
        connection.commit();
        return entries;
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readIds(Connection, List<Long>) error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + " readIds(Connection, List<Long>) connection error: " + e.getMessage(), e);
    }
    return new ArrayList<>();
  }

  private void retrieveCurrentOffset(final Connection connection) {
    this.offset = 1;

    try (final PreparedStatement selectCurrentOffset = queries.prepareSelectCurrentOffsetQuery(connection, name);
         final ResultSet resultSet = selectCurrentOffset.executeQuery()) {
      if (resultSet.next()) {
        this.offset = resultSet.getLong(1);
        connection.commit();
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
    }
  }

  private void updateCurrentOffset(final Connection connection) {
    try (final PreparedStatement upsertOffset = queries.prepareUpsertOffsetQuery(connection, name, offset)) {
      upsertOffset.executeUpdate();
      connection.commit();
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Could not persist the offset. Will retry on next read.");
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
    }
  }

  private long retrieveLastOffset(final Connection connection) {
    try (final PreparedStatement selectLastOffset = queries.prepareSelectLastOffsetQuery(connection);
         final ResultSet resultSet = selectLastOffset.executeQuery()) {
      if (resultSet.next()) {
        final long lastOffset = resultSet.getLong(1);
        connection.commit();
        return lastOffset;
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Could not retrieve latest offset, using current.");
    }

    return offset;
  }

  public static class JDBCJournalReaderInstantiator implements ActorInstantiator<JDBCJournalReaderActor> {
    private static final long serialVersionUID = -7848399986246046163L;

    private final Configuration configuration;
    private final String name;

    public JDBCJournalReaderInstantiator(final Configuration configuration, final String name) {
      this.configuration = configuration;
      this.name = name;
    }

    @Override
    public JDBCJournalReaderActor instantiate() {
      try {
        return new JDBCJournalReaderActor(configuration, name);
      } catch (SQLException e) {
        throw new IllegalArgumentException("Failed instantiator of " + getClass() + " because: " + e.getMessage(), e);
      }
    }

    @Override
    public Class<JDBCJournalReaderActor> type() {
      return JDBCJournalReaderActor.class;
    }
  }
}
