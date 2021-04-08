// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
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
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.journal.JournalReader;

public class JDBCJournalReaderActor extends Actor implements JournalReader<TextEntry> {
    private final Connection connection;
    private final EntryAdapterProvider entryAdapterProvider;
    private final DatabaseType databaseType;
    private final Gson gson;
    private final String name;
    private final JDBCQueries queries;

    private GapRetryReader<TextEntry> reader = null;

    private long offset;

    public JDBCJournalReaderActor(final Configuration configuration, final String name) throws SQLException {
        this.connection = configuration.connection;
        this.databaseType = configuration.databaseType;
        this.name = name;
        this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());

        this.queries = JDBCQueries.queriesFor(this.connection);

        this.gson = new Gson();
        retrieveCurrentOffset();
    }

    @Override
    public void close() {
      try {
        queries.close();
      } catch (SQLException e) {
        // ignore
      }
    }

    @Override
    public Completes<String> name() {
        return completes().with(name);
    }

    @Override
    public Completes<TextEntry> readNext() {
        try (final ResultSet resultSet = queries.prepareSelectEntryQuery(offset).executeQuery()) {
            if (resultSet.next()) {
                TextEntry entry = entryFromResultSet(resultSet);

                ++offset;
                updateCurrentOffset();
                return completes().with(entry);
            } else {
                List<Long> gapIds = reader().detectGaps(null, offset);
                GappedEntries<TextEntry> gappedEntries = new GappedEntries<>(new ArrayList<>(), gapIds, completesEventually());
                reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

                ++offset;
                updateCurrentOffset();
                return completes();
            }
        } catch (Exception e) {
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
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
        try (final ResultSet resultSet = queries.prepareSelectEntryBatchQuery(offset, maximumEntries).executeQuery()) {
            final List<TextEntry> entries = entriesFromResultSet(resultSet);
            List<Long> gapIds = reader().detectGaps(entries, offset, maximumEntries);
            if (!gapIds.isEmpty()) {
                GappedEntries<TextEntry> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
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
        } catch (Exception e) {
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
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
        updateCurrentOffset();
    }

    @Override
    public Completes<String> seekTo(final String id) {
        switch (id) {
            case Beginning:
                this.offset = 1;
                updateCurrentOffset();
                break;
            case End:
                this.offset = retrieveLastOffset() + 1;
                updateCurrentOffset();
                break;
            case Query:
                break;
            default:
                this.offset = Integer.parseInt(id);
                updateCurrentOffset();
                break;
        }

        return completes().with(String.valueOf(offset));
    }

    @Override
    public Completes<Long> size() {
        try (final ResultSet resultSet = queries.prepareSelectJournalCount().executeQuery()) {
          if (resultSet.next()) {
              final long count = resultSet.getLong(1);
              connection.commit();
              return completes().with(count);
          }
        } catch (Exception e) {
          logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
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

    private List<TextEntry> readIds(List<Long> ids) {
        try (PreparedStatement statement = queries.prepareNewSelectEntriesByIdsQuery(ids);
             final ResultSet resultSet = statement.executeQuery()) {
            final List<TextEntry> entries = entriesFromResultSet(resultSet);
            return entries;
        } catch (Exception e) {
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    private void retrieveCurrentOffset() {
        this.offset = 1;

        try (final ResultSet resultSet = queries.prepareSelectCurrentOffsetQuery(name).executeQuery()) {
            if (resultSet.next()) {
                this.offset = resultSet.getLong(1);
                connection.commit();
            }
        } catch (Exception e) {
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Rewinding the offset");
        }
    }

    private void updateCurrentOffset() {
        try {
            queries.prepareUpsertOffsetQuery(name, offset).executeUpdate();
            connection.commit();
        } catch (Exception e) {
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": Could not persist the offset. Will retry on next read.");
            logger().error("xoom-symbio-jdbc:journal-reader-" + databaseType + ": " + e.getMessage(), e);
        }
    }

    private long retrieveLastOffset() {
        try (final ResultSet resultSet = queries.prepareSelectLastOffsetQuery().executeQuery()) {
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
