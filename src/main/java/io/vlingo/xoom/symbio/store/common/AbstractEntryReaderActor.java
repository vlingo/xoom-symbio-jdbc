// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.store.EntryReaderStream;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.journal.JournalReader;
import io.vlingo.xoom.symbio.store.state.StateStoreEntryReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {
  private final Advice advice;
  private final String name;
  private final Configuration configuration;
  private final EntryAdapterProvider entryAdapterProvider;

  private GapRetryReader<T> reader = null;

  private long currentId = 0L;

  public AbstractEntryReaderActor(final Advice advice, final String name) {
    this.advice = advice;
    this.name = name;
    this.configuration = advice.specificConfiguration();
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
  }

  /**
   * Read one entry from the {@link ResultSet}.
   *
   * @param result the ResultSet from which to read
   * @return T
   * @throws Exception if the read fails
   */
  protected abstract T entryFrom(final ResultSet result) throws Exception;

  @Override
  public void close() {
    //
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<T> readNext() {
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement queryOne = connection.prepareStatement(this.advice.queryEntryExpression)) {
        queryOne.clearParameters();
        queryOne.setLong(1, currentId);
        try (final ResultSet result = queryOne.executeQuery()) {
          if (result.first()) {
            final T entry = entryFrom(result);

            ++currentId;
            connection.commit();
            return completes().with(entry);
          } else {
            List<Long> gapIds = reader().detectGaps(null, currentId);
            GappedEntries<T> gappedEntries = new GappedEntries<>(new ArrayList<>(), gapIds, completesEventually());
            reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

            ++currentId;
            connection.commit();
            return completes();
          }
        }
      } catch (Exception e) {
        connection.rollback();
        logger().error("Unable to read next entry for " + name + " because: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("Unable (connection) to read next entry for " + name + " because: " + e.getMessage(), e);
    }

    return completes().with(null);
  }

  @Override
  public Completes<List<T>> readNext(final int maximumEntries) {
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement queryBatch = connection.prepareStatement(this.advice.queryEntryBatchExpression)) {
        queryBatch.clearParameters();
        queryBatch.setLong(1, currentId);
        queryBatch.setInt(2, maximumEntries);
        try (final ResultSet result = queryBatch.executeQuery()) {
          final List<T> entries = mapQueriedEntriesFrom(result);
          List<Long> gapIds = reader().detectGaps(entries, currentId, maximumEntries);
          if (!gapIds.isEmpty()) {
            GappedEntries<T> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
            reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

            // Move offset with maximumEntries regardless of filled up gaps
            currentId += maximumEntries;
            connection.commit();
            return completes();
          } else {
            currentId += maximumEntries;
            connection.commit();
            return completes().with(entries);
          }
        }
      } catch (Exception e) {
        connection.rollback();
        logger().error("Unable to read next " + maximumEntries + " entries for " + name + " because: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("Unable (connection) to read next " + maximumEntries + " entries for " + name + " because: " + e.getMessage(), e);
    }

    return completes().with(new ArrayList<>());
  }

  @Override
  public Completes<List<T>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public Completes<String> seekTo(final String id) {
    switch (id) {
      case Beginning:
        this.currentId = 1;
        updateCurrentOffset();
        break;
      case End:
        this.currentId = retrieveLatestOffset() + 1;
        updateCurrentOffset();
        break;
      case Query:
        break;
      default:
        this.currentId = Integer.parseInt(id);
        updateCurrentOffset();
        break;
    }

    return completes().with(String.valueOf(currentId));
  }

  @Override
  public Completes<T> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  public void rewind() {
    currentId = 0;
  }

  @Override
  public Completes<Long> size() {
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement queryCount = connection.prepareStatement(this.advice.queryCount);
           final ResultSet resultSet = queryCount.executeQuery()) {
        if (resultSet.next()) {
          final long count = resultSet.getLong(1);
          connection.commit();
          return completes().with(count);
        }

        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc: " + e.getMessage(), e);
        logger().error("xoom-symbio-jdbc: Rewinding the offset");
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc (connection): " + e.getMessage(), e);
      logger().error("xoom-symbio-jdbc: Rewinding the offset");
    }

    return completes().with(-1L);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<Stream> streamAll() {
    return completes().with(new EntryReaderStream<>(stage(), selfAs(JournalReader.class), entryAdapterProvider));
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  private List<T> mapQueriedEntriesFrom(final ResultSet result) throws Exception {
    final List<T> entries = new ArrayList<>();
    while (result.next()) {
      final T entry = entryFrom(result);
      entries.add(entry);
    }

    return entries;
  }

  private PreparedStatement prepareQueryByIdsStatement(Connection connection, String query, List<Long> ids) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    for (int i = 0; i < ids.size(); i++) {
      // parameter index starts from 1
      statement.setLong(i + 1, ids.get(i));
    }

    return statement;
  }

  private String queryByIds(String queryTemplate, int idsCount) {
    String[] placeholderList = new String[idsCount];
    Arrays.fill(placeholderList, "?");
    String placeholders = String.join(", ", placeholderList);

    return MessageFormat.format(queryTemplate, placeholders);
  }

  private List<T> readIds(List<Long> ids) {
    String query = queryByIds(advice.queryEntryIdsExpression, ids.size());
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement statement = prepareQueryByIdsStatement(connection, query, ids);
           final ResultSet result = statement.executeQuery()) {
        connection.commit();
        return mapQueriedEntriesFrom(result);
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc (connection) error: " + e.getMessage(), e);
    }

    return new ArrayList<>();
  }

  private GapRetryReader<T> reader() {
    if (reader == null) {
      reader = new GapRetryReader<>(stage(), scheduler());
    }

    return reader;
  }

  private long retrieveLatestOffset() {
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement queryLatestOffset = connection.prepareStatement(this.advice.queryLatestOffset)) {
        queryLatestOffset.clearParameters();
        queryLatestOffset.setString(1, name);
        try (ResultSet resultSet = queryLatestOffset.executeQuery()) {
          if (resultSet.next()) {
            connection.commit();
            return resultSet.getLong(1);
          }
        }
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc error: Could not retrieve latest offset, using current.");
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc (connection) error: Could not retrieve latest offset, using current.");
    }

    return 0;
  }

  private void updateCurrentOffset() {
    try (final Connection connection = configuration.connectionProvider.newConnection()) {
      try (final PreparedStatement updateCurrentOffset = connection.prepareStatement(this.advice.queryUpdateCurrentOffset)) {
        updateCurrentOffset.clearParameters();
        updateCurrentOffset.setLong(1, currentId);
        updateCurrentOffset.setString(2, name);
        updateCurrentOffset.setLong(3, currentId);

        updateCurrentOffset.executeUpdate();
        connection.commit();
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc error: Could not persist the offset. Will retry on next read.");
        logger().error("xoom-symbio-jdbc: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc (connection) error: Could not persist the offset. Will retry on next read.");
      logger().error("xoom-symbio-jdbc: " + e.getMessage(), e);
    }
  }
}
