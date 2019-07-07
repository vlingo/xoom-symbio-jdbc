// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.common.Tuple2;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PostgresJournalActor extends Actor implements Journal<String> {
  private static final String INSERT_EVENT =
          "INSERT INTO vlingo_symbio_journal(entry_data, entry_metadata, entry_type, entry_type_version, stream_name, stream_version, id, entry_timestamp)"
                  + " VALUES(?::JSONB, ?::JSONB, ?, ?, ?, ?, ?, ?)";

  private static final String INSERT_SNAPSHOT =
          "INSERT INTO vlingo_symbio_journal_snapshots(stream_name, snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata)"
                  + " VALUES(?, ?, ?, ?::JSONB, ?, ?::JSONB)";

  private final static String INSERT_DISPATCHABLE = "INSERT INTO vlingo_symbio_journal_dispatch \n" +
          "(d_id, d_created_at, d_originator_id, d_dispatch_id, \n" +
          " d_state_id, d_state_type, d_state_type_version, \n" +
          " d_state_data, d_state_data_version, \n" +
          " d_state_metadata, d_entries) \n" +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSONB, ?, ?::JSONB, ?)";


  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final Configuration configuration;
  private final Connection connection;
  private final PreparedStatement insertEvent;
  private final PreparedStatement insertSnapshot;
  private final PreparedStatement insertDispatchable;
  private final Gson gson;
  private final Map<String, JournalReader<TextEntry>> journalReaders;
  private final Map<String, StreamReader<String>> streamReaders;
  private final IdentityGenerator identityGenerator;
  private final IdentityGenerator dispatchablesIdentityGenerator;
  private final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher;
  private final DispatcherControl dispatcherControl;

  public PostgresJournalActor(final Configuration configuration) throws SQLException {
    this(null, configuration, 0L, 0L);
  }

  public PostgresJournalActor(final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher, final Configuration configuration) throws SQLException {
    this(dispatcher, configuration, 1000L, 1000L);
  }

  public PostgresJournalActor(final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher, final Configuration configuration,
          final long checkConfirmationExpirationInterval, final long confirmationExpiration) throws SQLException {
    this.configuration = configuration;
    this.connection = configuration.connection;

    this.insertEvent = connection.prepareStatement(INSERT_EVENT);
    this.insertSnapshot = connection.prepareStatement(INSERT_SNAPSHOT);
    this.insertDispatchable = connection.prepareStatement(INSERT_DISPATCHABLE);

    this.gson = new Gson();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.journalReaders = new HashMap<>();
    this.streamReaders = new HashMap<>();

    this.identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();
    this.dispatchablesIdentityGenerator = new IdentityGenerator.RandomIdentityGenerator();
    if (dispatcher != null) {
      this.dispatcher = dispatcher;
      final PostgresDispatcherControlDelegate dispatcherControlDelegate = new PostgresDispatcherControlDelegate(connection,
              configuration.originatorId, stage().world().defaultLogger());
      this.dispatcherControl = stage().actorFor(DispatcherControl.class,
              Definition.has(DispatcherControlActor.class,
                  Definition.parameters(dispatcher,
                          dispatcherControlDelegate,
                          checkConfirmationExpirationInterval,
                          confirmationExpiration)
              )
      );
    } else {
      this.dispatcher = null;
      this.dispatcherControl = null;
    }
  }

  @Override
  public void stop() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    super.stop();
  }

  @Override
  public <S, ST> void append(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata,
          final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendResultedInFailure(streamName, streamVersion, source, null, interest, object, e);
    final Entry<String> entry = asEntry(source, metadata, whenFailed);
    insertEntry(streamName, streamVersion, entry, whenFailed);
    doCommit(whenFailed);
    dispatch(streamName, streamVersion, Collections.singletonList(entry), null, whenFailed);
    interest.appendResultedIn(Success.of(Result.Success), streamName, streamVersion, source, Optional.empty(), object);
  }

  @Override
  public <S, ST> void appendWith(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata, final ST snapshot,
          final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendResultedInFailure(streamName, streamVersion, source, snapshot, interest, object, e);
    final Entry<String> entry = asEntry(source, metadata, whenFailed);
    insertEntry(streamName, streamVersion, entry, whenFailed);
    final Tuple2<Optional<ST>, Optional<TextState>> snapshotState = toState(streamName, snapshot, streamVersion);
    snapshotState._2.ifPresent(state -> insertSnapshot(streamName, state, whenFailed));
    doCommit(whenFailed);
    dispatch(streamName, streamVersion, Collections.singletonList(entry), snapshotState._2.orElse(null), whenFailed);
    interest.appendResultedIn(Success.of(Result.Success), streamName, streamVersion, source, snapshotState._1, object);
  }

  @Override
  public <S, ST> void appendAll(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final Metadata metadata,
          final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, null, interest, object, e);
    final List<Entry<String>> entries = asEntries(sources, metadata, whenFailed);
    int version = fromStreamVersion;
    for (final Entry<String> entry : entries) {
      insertEntry(streamName, version++, entry, whenFailed);
    }
    doCommit(whenFailed);
    dispatch(streamName, fromStreamVersion, entries, null, whenFailed);
    interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.empty(), object);
  }

  @Override
  public <S, ST> void appendAllWith(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final Metadata metadata,
          final ST snapshot, final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, snapshot, interest, object, e);
    final List<Entry<String>> entries = asEntries(sources, metadata, whenFailed);
    int version = fromStreamVersion;
    for (final Entry<String> entry : entries) {
      insertEntry(streamName, version++, entry, whenFailed);
    }
    final Tuple2<Optional<ST>, Optional<TextState>> snapshotState = toState(streamName, snapshot, fromStreamVersion);
    snapshotState._2.ifPresent(state -> insertSnapshot(streamName, state, whenFailed));
    doCommit(whenFailed);
    dispatch(streamName, fromStreamVersion, entries, snapshotState._2.orElse(null), whenFailed);
    interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, snapshotState._1, object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<JournalReader<TextEntry>> journalReader(final String name) {
    final JournalReader<TextEntry> reader = journalReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
      return stage().actorFor(JournalReader.class, Definition.has(PostgresJournalReaderActor.class, Definition.parameters(configuration, name)), address);
    });

    return completes().with(reader);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<StreamReader<String>> streamReader(final String name) {
    final StreamReader<String> reader = streamReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
      return stage().actorFor(StreamReader.class, Definition.has(PostgresStreamReaderActor.class, Definition.parameters(configuration)), address);
    });

    return completes().with(reader);
  }

  protected final void insertEntry(final String streamName, final int streamVersion, final Entry<String> entry, final Consumer<Exception> whenFailed) {
    try {
      final UUID id = identityGenerator.generate();
      final long timestamp = id.timestamp();

      insertEvent.setString(1, entry.entryData());
      insertEvent.setString(2, gson.toJson(entry.metadata()));
      insertEvent.setString(3, entry.type());
      insertEvent.setInt(4, entry.typeVersion());
      insertEvent.setString(5, streamName);
      insertEvent.setInt(6, streamVersion);
      insertEvent.setObject(7, id);
      insertEvent.setLong(8, timestamp);

      if (insertEvent.executeUpdate() != 1) {
        logger().error("vlingo/symbio-jdbc-postgres: Could not insert event " + entry.toString());
        throw new IllegalStateException("vlingo/symbio-jdbc-postgres: Could not insert event");
      }

      ((BaseEntry<String>) entry).__internal__setId(id.toString()); //questionable cast
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo/symbio-jdbc-postgres: Could not insert event " + entry.toString(), e);
      throw new IllegalStateException(e);
    }
  }

  protected final void insertSnapshot(final String eventStream, final TextState snapshotState, final Consumer<Exception> whenFailed) {
    try {
      insertSnapshot.setString(1, eventStream);
      insertSnapshot.setString(2, snapshotState.type);
      insertSnapshot.setInt(3, snapshotState.typeVersion);
      insertSnapshot.setString(4, snapshotState.data);
      insertSnapshot.setInt(5, snapshotState.dataVersion);
      insertSnapshot.setString(6, gson.toJson(snapshotState.metadata));

      if (insertSnapshot.executeUpdate() != 1) {
        logger().error("vlingo/symbio-jdbc-postgres: Could not insert snapshot with id " + snapshotState.id);
        throw new IllegalStateException("vlingo/symbio-jdbc-postgres: Could not insert snapshot");
      }
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo/symbio-jdbc-postgres: Could not insert event with id " + snapshotState.id, e);
      throw new IllegalStateException(e);
    }
  }

  protected final void insertDispatchable(final Dispatchable<Entry<String>, TextState> dispatchable, final Consumer<Exception> whenFailed) {
    try {
      final State<String> state = dispatchable.typedState();
      final UUID id = identityGenerator.generate();
      insertDispatchable.clearParameters();
      insertDispatchable.setObject(1, id);
      insertDispatchable.setObject(2, Timestamp.valueOf(LocalDateTime.now()));
      insertDispatchable.setString(3, configuration.originatorId);
      insertDispatchable.setString(4, dispatchable.id());
      if (dispatchable.state().isPresent()) {
        insertDispatchable.setString(5, state.id);
        insertDispatchable.setString(6, state.type);
        insertDispatchable.setInt(7, state.typeVersion);
        insertDispatchable.setString(8, state.data);
        insertDispatchable.setInt(9, state.dataVersion);
        insertDispatchable.setString(10, gson.toJson(state.metadata));
      } else {
        insertDispatchable.setString(5, null);
        insertDispatchable.setString(6, null);
        insertDispatchable.setInt(7, 0);
        insertDispatchable.setString(8, null);
        insertDispatchable.setInt(9, 0);
        insertDispatchable.setString(10, null);
      }
      if (dispatchable.entries() != null && !dispatchable.entries().isEmpty()) {
        insertDispatchable.setString(11,
                dispatchable.entries().stream().map(Entry::id).collect(Collectors.joining(PostgresDispatcherControlDelegate.DISPATCHEABLE_ENTRIES_DELIMITER)));
      } else {
        insertDispatchable.setString(11, "");
      }

      if (insertDispatchable.executeUpdate() != 1) {
        logger().error("vlingo/symbio-jdbc-postgres: Could not insert dispatchable with id " + dispatchable.id());
        throw new IllegalStateException("vlingo/symbio-jdbc-postgres: Could not insert snapshot");
      }
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo/symbio-jdbc-postgres: Could not insert dispatchable with id " + dispatchable.id(), e);
      throw new IllegalStateException(e);
    }
  }

  private <S, ST> void appendResultedInFailure(final String streamName, final int streamVersion, final Source<S> source, final ST snapshot,
          final AppendResultInterest interest, final Object object, final Exception e) {

    interest.appendResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), streamName, streamVersion, source,
            snapshot == null ? Optional.empty() : Optional.of(snapshot), object);
  }

  private <S, ST> void appendAllResultedInFailure(final String streamName, final int streamVersion, final List<Source<S>> sources, final ST snapshot,
          final AppendResultInterest interest, final Object object, final Exception e) {

    interest.appendAllResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), streamName, streamVersion, sources,
            snapshot == null ? Optional.empty() : Optional.of(snapshot), object);
  }

  private void doCommit(final Consumer<Exception> whenFailed) {
    try {
      connection.commit();
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo/symbio-jdbc-postgres: Could not complete transaction", e);
      throw new IllegalStateException(e);
    }
  }

  private <S> List<Entry<String>> asEntries(final List<Source<S>> sources, final Metadata metadata, final Consumer<Exception> whenFailed) {
    final List<Entry<String>> entries = new ArrayList<>(sources.size());
    for (final Source<?> source : sources) {
      entries.add(asEntry(source, metadata, whenFailed));
    }
    return entries;
  }

  private <S> Entry<String> asEntry(final Source<S> source, final Metadata metadata, final Consumer<Exception> whenFailed) {
    try {
      return entryAdapterProvider.asEntry(source, metadata);
    } catch (final Exception e) {
      whenFailed.accept(e);
      logger().error("vlingo/symbio-jdbc-postgres: Cannot adapt source to entry because: ", e);
      throw new IllegalArgumentException(e);
    }
  }

  private <ST> Tuple2<Optional<ST>, Optional<TextState>> toState(final String streamName, final ST snapshot, final int streamVersion) {
    if (snapshot != null) {
      return Tuple2.from(Optional.of(snapshot), Optional.of(stateAdapterProvider.asRaw(streamName, snapshot, streamVersion)));
    } else {
      return Tuple2.from(Optional.empty(), Optional.empty());
    }
  }

  private void dispatch(final String streamName, final int streamVersion, final List<Entry<String>> entries, final TextState snapshot,
          final Consumer<Exception> whenFailed) {
    if (dispatcher != null) {
      final String id = getDispatchId(streamName, streamVersion);
      final Dispatchable<Entry<String>, TextState> dispatchable = new Dispatchable<>(id, LocalDateTime.now(), snapshot, entries);
      insertDispatchable(dispatchable, whenFailed);
      //dispatch only if insert successful
      this.dispatcher.dispatch(dispatchable);
    }
  }

  private String getDispatchId(final String streamName, final int streamVersion) {
    return streamName + ":" + streamVersion + ":" + dispatchablesIdentityGenerator.generate().toString();
  }
}
