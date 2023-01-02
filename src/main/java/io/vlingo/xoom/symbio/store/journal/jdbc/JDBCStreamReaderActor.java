// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import static java.util.Collections.emptyList;

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
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.journal.EntityStream;
import io.vlingo.xoom.symbio.store.journal.StreamReader;

public class JDBCStreamReaderActor extends Actor implements StreamReader<String> {
  private final ConnectionProvider connectionProvider;
  private final DatabaseType databaseType;
  private final Gson gson;
  private final JDBCQueries queries;

  public JDBCStreamReaderActor(final Configuration configuration) throws SQLException {
    this.connectionProvider = configuration.connectionProvider;
    this.databaseType = configuration.databaseType;
    this.gson = new Gson();

    try (Connection initConnection = connectionProvider.newConnection()) {
      try {
        this.queries = JDBCQueries.queriesFor(initConnection);
        initConnection.commit();
      } catch (Exception e) {
        initConnection.rollback();
        throw new IllegalStateException("Failed to initialize JDBCStreamReaderActor because: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public Completes<EntityStream<String>> streamFor(final String streamName) {
    return streamFor(streamName, 1);
  }

  @Override
  public Completes<EntityStream<String>> streamFor(final String streamName, final int fromStreamVersion) {
    try (Connection connection = connectionProvider.newConnection()) {
      try {
        final EntityStream<String> steamStream = eventsFromOffset(connection, streamName, fromStreamVersion);
        connection.commit();
        return completes().with(steamStream);
      } catch (Exception e) {
        connection.rollback();
        logger().error("xoom-symbio-jdbc:journal-stream-reader-" + databaseType + " error: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      logger().error("xoom-symbio-jdbc:journal-stream-reader-" + databaseType + " connection error: " + e.getMessage(), e);
    }

    return completes().with(new EntityStream<>(streamName, 1, emptyList(), TextState.Null));
  }

  @Override
  public void stop() {
    // no resources to be closed
    super.stop();
  }

  private EntityStream<String> eventsFromOffset(final Connection connection, final String streamName, final int offset) throws Exception {
    final State<String> snapshot = latestSnapshotOf(connection, streamName);
    final List<BaseEntry<String>> events = new ArrayList<>();

    int dataVersion = offset;
    State<String> referenceSnapshot = TextState.Null;

    if (snapshot != TextState.Null) {
      if (snapshot.dataVersion > offset) {
        dataVersion = snapshot.dataVersion;
        referenceSnapshot = snapshot;
      }
    }

    int fullStreamVersion = 0;

    try (final PreparedStatement selectStream = queries.prepareSelectStreamQuery(connection, streamName, dataVersion);
         final ResultSet resultSet = selectStream.executeQuery()) {
      while (resultSet.next()) {
        final String id = resultSet.getString(1);
        final int streamVersion = resultSet.getInt(2);
        fullStreamVersion = streamVersion;
        final String entryData = resultSet.getString(3);
        final String entryType = resultSet.getString(4);
        final int eventTypeVersion = resultSet.getInt(5);
        final String entryMetadata = resultSet.getString(6);

        final Class<?> classOfEvent = StoredTypes.forName(entryType);
        final Metadata eventMetadataDeserialized = gson.fromJson(entryMetadata, Metadata.class);

        events.add(new BaseEntry.TextEntry(id, classOfEvent, eventTypeVersion, entryData, eventMetadataDeserialized));
      }
    }

    return new EntityStream<>(streamName, fullStreamVersion, events, referenceSnapshot);
  }

  private State<String> latestSnapshotOf(final Connection connection, final String streamName) throws Exception {
    try (final PreparedStatement selectSnapshot = queries.prepareSelectSnapshotQuery(connection, streamName);
         final ResultSet resultSet = selectSnapshot.executeQuery()) {
      if (resultSet.next()) {
        final String snapshotData = resultSet.getString(1);
        final int snapshotDataVersion = resultSet.getInt(2);
        final String snapshotDataType = resultSet.getString(3);
        final int snapshotDataTypeVersion = resultSet.getInt(4);
        final String metadataJson = resultSet.getString(5);

        final Class<?> snapshotDataTypeClass = StoredTypes.forName(snapshotDataType);
        final Metadata eventMetadataDeserialized = gson.fromJson(metadataJson, Metadata.class);

        return new State.TextState(streamName, snapshotDataTypeClass, snapshotDataTypeVersion, snapshotData, snapshotDataVersion, eventMetadataDeserialized);
      }
      return TextState.Null;
    }
  }

  public static class JDBCStreamReaderInstantiator implements ActorInstantiator<JDBCStreamReaderActor> {
    private static final long serialVersionUID = -560289226104663046L;

    private final Configuration configuration;

    public JDBCStreamReaderInstantiator(final Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public JDBCStreamReaderActor instantiate() {
      try {
        return new JDBCStreamReaderActor(configuration);
      } catch (SQLException e) {
        throw new IllegalArgumentException("Failed instantiator of " + getClass() + " because: " + e.getMessage(), e);
      }
    }

    @Override
    public Class<JDBCStreamReaderActor> type() {
      return JDBCStreamReaderActor.class;
    }
  }
}
