// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry.BinaryEntry;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class PostgresStateStoreEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {
  private final Advice advice;
  private final Configuration configuration;
  private long currentId;
  private final String name;
  private final PreparedStatement queryBatch;
  private final PreparedStatement queryOne;

  public PostgresStateStoreEntryReaderActor(final Advice advice, final String name) throws Exception {
    this.advice = advice;
    this.name = name;
    this.configuration = advice.specificConfiguration();
    this.currentId = 0;

    this.queryBatch = configuration.connection.prepareStatement(this.advice.queryEntryBatchExpression);
    this.queryOne = configuration.connection.prepareStatement(this.advice.queryEntryExpression);
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
  @SuppressWarnings("unchecked")
  public Completes<List<T>> readNext(int maximumEntries) {
    return completes().with((List<T>) queryNext(maximumEntries));
  }

  @Override
  public void rewind() {
    currentId = 0;
  }

  @Override
  public Completes<String> seekTo(String id) {
    return null;
  }

  private Entry<?> queryNext() {
    try {
      queryOne.clearParameters();
      queryOne.setLong(1, currentId);
      final ResultSet result = queryOne.executeQuery();
      if (result.first()) {
        final long id = result.getLong(1);
        final Entry<?> entry = entryFrom(result, id);
        currentId = id + 1L;
        return entry;
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
      final ResultSet result = queryBatch.executeQuery();
      final List<Entry<?>> entries = new ArrayList<>(maximumEntries);
      while (result.next()) {
        final long id = result.getLong(1);
        final Entry<?> entry = entryFrom(result, id);
        currentId = id + 1L;
        entries.add(entry);
      }
      return entries;
    } catch (Exception e) {
      logger().error("Unable to read next " + maximumEntries + " entries for " + name + " because: " + e.getMessage(), e);
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
}
