// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import com.google.gson.Gson;
import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.common.Success;
import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.common.identity.IdentityGenerator;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JDBCJournalInstantWriter implements JDBCJournalWriter {
  private final Configuration configuration;
  private final ConnectionProvider connectionProvider;
  private final JDBCQueries queries;
  private final List<Dispatcher<Dispatchable<Entry<String>, TextState>>> dispatchers;
  private final DispatcherControl dispatcherControl;
  private final Gson gson;
  private final IdentityGenerator dispatchablesIdentityGenerator;

  private Logger logger;

  public JDBCJournalInstantWriter(Configuration configuration, List<Dispatcher<Dispatchable<Entry<String>, TextState>>> dispatchers,
                                  DispatcherControl dispatcherControl) throws Exception {
    this.configuration = configuration;
    this.connectionProvider = configuration.connectionProvider;
    this.dispatchers = dispatchers;
    this.dispatcherControl = dispatcherControl;
    this.gson = new Gson();
    this.dispatchablesIdentityGenerator = new IdentityGenerator.RandomIdentityGenerator();

    try (Connection initConnection = connectionProvider.newConnection()) {
      try {
        this.queries = JDBCQueries.queriesFor(initConnection);
        initConnection.commit();
      } catch (Exception e) {
        initConnection.rollback();
        throw new IllegalStateException("Failed to initialize JDBCJournalInstantWriter because: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public void appendEntry(String streamName, int streamVersion, Entry<String> entry, Optional<TextState> snapshotState,
                          Consumer<Outcome<StorageException, Result>> postAppendAction) {
    try (final Connection connection = connectionProvider.newConnection()) {
      try {
        insertEntry(connection, streamName, streamVersion, entry, postAppendAction);
        snapshotState.ifPresent(state -> insertSnapshot(connection, streamName, streamVersion, state, postAppendAction));
        final Dispatchable<Entry<String>, TextState> dispatchable =
            insertDispatchable(connection, streamName, streamVersion, Collections.singletonList(entry), snapshotState.orElse(null), postAppendAction);
        doCommit(connection, postAppendAction);
        dispatch(dispatchable);
        postAppendAction.accept(Success.of(Result.Success));
      } catch (Exception e) {
        connection.rollback();
        logger.error("xoom-symbio-jdbc:journal-" + configuration.databaseType + " error: Could not append entry to stream " + streamName, e);
      }
    } catch (Exception e) {
      logger.error("xoom-symbio-jdbc:journal-" + configuration.databaseType + " connection error: Could not append entry to stream " + streamName, e);
    }
  }

  @Override
  public void appendEntries(String streamName, int fromStreamVersion, List<Entry<String>> entries, Optional<TextState> snapshotState,
                            Consumer<Outcome<StorageException, Result>> postAppendAction) {
    int version = fromStreamVersion;

    try (final Connection connection = connectionProvider.newConnection()) {
      try {
        for (Entry<String> entry : entries) {
          insertEntry(connection, streamName, version++, entry, postAppendAction);
        }

        snapshotState.ifPresent(state -> insertSnapshot(connection, streamName, fromStreamVersion, state, postAppendAction));
        final Dispatchable<Entry<String>, TextState> dispatchable =
            insertDispatchable(connection, streamName, fromStreamVersion, entries, snapshotState.orElse(null), postAppendAction);
        doCommit(connection, postAppendAction);
        dispatch(dispatchable);
        postAppendAction.accept(Success.of(Result.Success));
      } catch (Exception e) {
        connection.rollback();
        logger.error("xoom-symbio-jdbc:journal-" + configuration.databaseType + " error: Could not append entries to stream " + streamName, e);
      }
    } catch (SQLException e) {
      logger.error("xoom-symbio-jdbc:journal-" + configuration.databaseType + " connection error: Could not append entries to stream " + streamName, e);
    }
  }

  @Override
  public void flush() {
    // No flush; this is an instant writer
  }

  @Override
  public void stop() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
  }

  @Override
  public void setLogger(Logger logger) {
    this.logger = logger;
  }

  private String buildDispatchId(String streamName, int streamVersion) {
    return streamName + ":" + streamVersion + ":" + dispatchablesIdentityGenerator.generate().toString();
  }

  private void dispatch(final Dispatchable<Entry<String>, TextState> dispatchable) {
    if (dispatchers != null) {
      // dispatch only if insert successful
      this.dispatchers.forEach(d -> d.dispatch(dispatchable));
    }
  }

  private void doCommit(final Connection connection, final Consumer<Outcome<StorageException, Result>> postAppendAction) {
    try {
      connection.commit();
    } catch (final SQLException e) {
      postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
      logger.error("xoom-symbio-jdbc:journal-" + configuration.databaseType + ": Could not complete transaction", e);
      throw new IllegalStateException(e);
    }
  }

  private Dispatchable<Entry<String>, TextState> insertDispatchable(Connection connection, String streamName, int streamVersion, final List<Entry<String>> entries,
                                                                    TextState snapshotState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
    final String id = buildDispatchId(streamName, streamVersion);
    final Dispatchable<Entry<String>, TextState> dispatchable = new Dispatchable<>(id, LocalDateTime.now(), snapshotState, entries);
    String dbType = configuration.databaseType.toString();

    try {
      final String encodedEntries = dispatchable.hasEntries()
          ? dispatchable.entries().stream().map(Entry::id).collect(Collectors.joining(JDBCDispatcherControlDelegate.DISPATCHEABLE_ENTRIES_DELIMITER))
          : "";

      final Tuple2<PreparedStatement, Optional<String>> insertDispatchable;

      final String dispatchableId = dispatchable.id();

      if (dispatchable.state().isPresent()) {
        final State<String> state = dispatchable.typedState();

        insertDispatchable =
            queries.prepareInsertDispatchableQuery(
                connection,
                dispatchableId,
                configuration.originatorId,
                state.id,
                state.data,
                state.dataVersion,
                state.type,
                state.typeVersion,
                gson.toJson(state.metadata),
                encodedEntries);
      } else {
        insertDispatchable =
            queries.prepareInsertDispatchableQuery(
                connection,
                dispatchableId,
                configuration.originatorId,
                null,
                null,
                0,
                null,
                0,
                null,
                encodedEntries);
      }

      if (insertDispatchable._1.executeUpdate() != 1) {
        logger.error("xoom-symbio-jdbc:journal-" + dbType + ": Could not insert dispatchable with id " + dispatchable.id());
        throw new IllegalStateException("xoom-symbio-jdbc:journal-" + dbType + ": Could not insert snapshot");
      }
    } catch (final SQLException e) {
      postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
      logger.error("xoom-symbio-jdbc:journal-" + dbType + ": Could not insert dispatchable with id " + dispatchable.id(), e);
      throw new IllegalStateException(e);
    }

    return dispatchable;
  }

  private void insertSnapshot(final Connection connection, final String streamName, final int streamVersion, final TextState snapshotState,
                              final Consumer<Outcome<StorageException, Result>> postAppendAction) {
    DatabaseType databaseType = configuration.databaseType;

    try {
      final Tuple2<PreparedStatement, Optional<String>> insertSnapshot =
          queries.prepareInsertSnapshotQuery(
              connection,
              streamName,
              streamVersion,
              snapshotState.data,
              snapshotState.dataVersion,
              snapshotState.type,
              snapshotState.typeVersion,
              gson.toJson(snapshotState.metadata));

      if (insertSnapshot._1.executeUpdate() != 1) {
        logger.error("xoom-symbio-jdbc:journal-" + databaseType + ": Could not insert snapshot with id " + snapshotState.id);
        throw new IllegalStateException("xoom-symbio-jdbc:journal-" + databaseType + ": Could not insert snapshot");
      }
    } catch (final SQLException e) {
      postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
      logger.error("xoom-symbio-jdbc:journal-" + databaseType + ": Could not insert event with id " + snapshotState.id, e);
      throw new IllegalStateException(e);
    }
  }

  private void insertEntry(Connection connection, String streamName, int streamVersion, Entry<String> entry, Consumer<Outcome<StorageException, Result>> postAppendAction) {
    final DatabaseType databaseType = configuration.databaseType;

    try {
      final Tuple2<PreparedStatement, Optional<String>> insertEntry =
          queries.prepareInsertEntryQuery(
              connection,
              streamName,
              streamVersion,
              entry.entryData(),
              entry.typeName(),
              entry.typeVersion(),
              gson.toJson(entry.metadata()));

      if (insertEntry._1.executeUpdate() != 1) {
        logger.error("xoom-symbio-jdbc:journal-" + databaseType + ": Could not insert event " + entry.toString());
        throw new IllegalStateException("xoom-symbio-jdbc:journal-" + databaseType + ": Could not insert event");
      }

      if (insertEntry._2.isPresent()) {
        ((BaseEntry<String>) entry).__internal__setId(insertEntry._2.get());
      } else {
        final long id = queries.generatedKeyFrom(insertEntry._1);
        if (id > 0) {
          ((BaseEntry<String>) entry).__internal__setId(String.valueOf(id));
        }
      }
    } catch (final SQLException e) {
      postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
      logger.error("xoom-symbio-jdbc:journal-" + databaseType +": Could not insert event " + entry.toString(), e);
      throw new IllegalStateException(e);
    }
  }
}
