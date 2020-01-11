// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry.BinaryEntry;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

public class DbStateStoreEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {
  private final Advice advice;
  private final Configuration configuration;
  private long currentId;
  private final String name;
  private final PreparedStatement queryBatch;
  private final PreparedStatement queryCount;
  private final PreparedStatement queryLatestOffset;
  private final PreparedStatement queryOne;
  private final PreparedStatement updateCurrentOffset;

  public DbStateStoreEntryReaderActor(final EntryReader.Advice advice, final String name) throws Exception {
    this.advice = advice;
    this.name = name;
    this.configuration = advice.specificConfiguration();
    this.currentId = 0;

    this.queryBatch = configuration.connection.prepareStatement(this.advice.queryEntryBatchExpression);
    this.queryCount = configuration.connection.prepareStatement(this.advice.queryCount);
    this.queryLatestOffset = configuration.connection.prepareStatement(this.advice.queryLatestOffset);
    this.queryOne = configuration.connection.prepareStatement(this.advice.queryEntryExpression);
    this.updateCurrentOffset = configuration.connection.prepareStatement(this.advice.queryUpdateCurrentOffset);
  }

  @Override
  public void close() {
    try {
      queryBatch.close();
      queryOne.close();
      configuration.connection.close();
    } catch (SQLException e) {
      // ignore
    }
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<T> readNext() {
    return completes().with((T) queryNext());
  }

  @Override
  public Completes<T> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<List<T>> readNext(final int maximumEntries) {
    return completes().with((List<T>) queryNext(maximumEntries));
  }

  @Override
  public Completes<List<T>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {
    currentId = 0;
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
  public Completes<Long> size() {
      try (final ResultSet resultSet = queryCount.executeQuery()) {
        if (resultSet.next()) {
            final long count = resultSet.getLong(1);
            return completes().with(count);
        }
      } catch (Exception e) {
        logger().error("vlingo/symbio-postgres: " + e.getMessage(), e);
        logger().error("vlingo/symbio-postgres: Rewinding the offset");
      }

      return completes().with(-1L);
  }

  private Entry<?> queryNext() {
    try {
      queryOne.clearParameters();
      queryOne.setLong(1, currentId);
      try (final ResultSet result = queryOne.executeQuery()) {
        if (result.first()) {
          final long id = result.getLong(1);
          final Entry<?> entry = entryFrom(result, id);
          currentId = id + 1L;
          return entry;
        }
      }
    } catch (Exception e) {
      logger().error("Unable to read next entry for " + name + " because: " + e.getMessage(), e);
    }
    return null;
  }

  private List<Entry<?>> queryNext(final int maximumEntries) {
    try {
      queryBatch.clearParameters();
      queryBatch.setLong(1, currentId);
      queryBatch.setInt(2, maximumEntries);
      try (final ResultSet result = queryBatch.executeQuery()) {
        final List<Entry<?>> entries = new ArrayList<>(maximumEntries);
        while (result.next()) {
          final long id = result.getLong(1);
          final Entry<?> entry = entryFrom(result, id);
          currentId = id + 1L;
          entries.add(entry);
        }
        return entries;
      }
    } catch (Exception e) {
      logger().error("Unable to read next " + maximumEntries + " entries for " + name + " because: " + e.getMessage(),
              e);
    }
    return new ArrayList<>(0);
  }

  private Entry<?> entryFrom(final ResultSet result, final long id) throws Exception {
    final String type = result.getString(2);
    final int typeVersion = result.getInt(3);
    final String metadataValue = result.getString(5);
    final String metadataOperation = result.getString(6);

    final Metadata metadata = Metadata.with(metadataValue, metadataOperation);

    if (configuration.format.isBinary()) {
      return new BinaryEntry(String.valueOf(id), typed(type), typeVersion, binaryDataFrom(result, 4), metadata);
    } else {
      return new TextEntry(String.valueOf(id), typed(type), typeVersion, textDataFrom(result, 4), metadata);
    }
  }

  private byte[] binaryDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final Blob blob = resultSet.getBlob(columnIndex);
    final byte[] data = blob.getBytes(1, (int) blob.length());
    return data;
  }

  private String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final String data = resultSet.getString(columnIndex);
    return data;
  }

  private Class<?> typed(final String typeName) throws Exception {
    return Class.forName(typeName);
  }

  private long retrieveLatestOffset() {
      try {
          queryBatch.clearParameters();
          queryLatestOffset.setString(1, name);
          try (ResultSet resultSet = queryLatestOffset.executeQuery()) {
              if (resultSet.next()) {
                  return resultSet.getLong(1);
              }
          }
      } catch (Exception e) {
          logger().error("vlingo/symbio-postgres: Could not retrieve latest offset, using current.");
      }

      return 0;
  }

  private void updateCurrentOffset() {
      try {
          updateCurrentOffset.clearParameters();
          updateCurrentOffset.setLong(1, currentId);
          updateCurrentOffset.setString(2, name);
          updateCurrentOffset.setLong(3, currentId);

          updateCurrentOffset.executeUpdate();
          configuration.connection.commit();
      } catch (Exception e) {
          logger().error("vlingo/symbio-postgres: Could not persist the offset. Will retry on next read.");
          logger().error("vlingo/symbio-postgres: " + e.getMessage(), e);
      }
  }

  public static class DbStateStoreEntryReaderInstantiator<T extends Entry<?>> implements ActorInstantiator<DbStateStoreEntryReaderActor<T>> {
    private Advice advice;
    private String name;

    public DbStateStoreEntryReaderInstantiator() { }

    @Override
    public DbStateStoreEntryReaderActor<T> instantiate() {
      try {
        return new DbStateStoreEntryReaderActor<>(advice, name);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to instantiate " + getClass() + " because: " + e.getMessage(), e);
      }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Class<DbStateStoreEntryReaderActor<T>> type() {
      return (Class) DbStateStoreEntryReaderActor.class;
    }

    @Override
    public void set(final String name, final Object value) {
      switch (name) {
      case "advice":
        this.advice = (Advice) value;
        break;
      case "name":
        this.name = (String) value;
        break;
      }
    }
  }
}
