// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

public class JDBCJournalActor extends Actor implements Journal<String> {
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final Configuration configuration;
  private final Connection connection;
  private final Gson gson;
  private final Map<String, JournalReader<TextEntry>> journalReaders;
  private final Map<String, StreamReader<String>> streamReaders;
  private final IdentityGenerator dispatchablesIdentityGenerator;
  private final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher;
  private final DispatcherControl dispatcherControl;

  private final JDBCQueries queries;

  public JDBCJournalActor(final Configuration configuration) throws Exception {
    this(null, configuration, 0L, 0L);
  }

  public JDBCJournalActor(final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher, final Configuration configuration) throws Exception {
    this(dispatcher, configuration, 1000L, 1000L);
  }

  public JDBCJournalActor(final Dispatcher<Dispatchable<Entry<String>, TextState>> dispatcher, final Configuration configuration,
                          final long checkConfirmationExpirationInterval, final long confirmationExpiration) throws Exception {
    this.configuration = configuration;
    this.connection = configuration.connection;

    this.connection.setAutoCommit(false);

    this.queries = JDBCQueries.queriesFor(configuration.connection);

    this.queries.createTables();

    this.gson = new Gson();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.journalReaders = new HashMap<>();
    this.streamReaders = new HashMap<>();

    this.dispatchablesIdentityGenerator = new IdentityGenerator.RandomIdentityGenerator();

    if (dispatcher != null) {
      this.dispatcher = dispatcher;
      final JDBCDispatcherControlDelegate dispatcherControlDelegate =
              new JDBCDispatcherControlDelegate(Configuration.cloneOf(configuration), stage().world().defaultLogger());
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

    try {
      queries.close();
    } catch (SQLException e) {
      // ignore
    }

    super.stop();
  }

  @Override
  public <S, ST> void append(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata,
          final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendResultedInFailure(streamName, streamVersion, source, null, interest, object, e);
    final Entry<String> entry = asEntry(source, metadata, whenFailed);
    insertEntry(streamName, streamVersion, entry, whenFailed);
    final Dispatchable<Entry<String>, TextState> dispatchable = buildDispatchable(streamName, streamVersion, Collections.singletonList(entry), null);
    insertDispatchable(dispatchable, whenFailed);

    doCommit(whenFailed);
    dispatch(dispatchable);
    interest.appendResultedIn(Success.of(Result.Success), streamName, streamVersion, source, Optional.empty(), object);
  }

  @Override
  public <S, ST> void appendWith(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata, final ST snapshot,
          final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = (e) -> appendResultedInFailure(streamName, streamVersion, source, snapshot, interest, object, e);
    final Entry<String> entry = asEntry(source, metadata, whenFailed);
    insertEntry(streamName, streamVersion, entry, whenFailed);
    final Tuple2<Optional<ST>, Optional<TextState>> snapshotState = toState(streamName, snapshot, streamVersion);
    snapshotState._2.ifPresent(state -> insertSnapshot(streamName, streamVersion, state, whenFailed));

    final Dispatchable<Entry<String>, TextState> dispatchable = buildDispatchable(streamName, streamVersion,
            Collections.singletonList(entry), snapshotState._2.orElse(null));
    insertDispatchable(dispatchable, whenFailed);

    doCommit(whenFailed);

    dispatch(dispatchable);
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
    final Dispatchable<Entry<String>, TextState> dispatchable = buildDispatchable(streamName, fromStreamVersion, entries, null);
    insertDispatchable(dispatchable, whenFailed);

    doCommit(whenFailed);

    dispatch(dispatchable);
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
    snapshotState._2.ifPresent(state -> insertSnapshot(streamName, fromStreamVersion, state, whenFailed));

    final Dispatchable<Entry<String>, TextState> dispatchable = buildDispatchable(streamName, fromStreamVersion, entries, snapshotState._2.orElse(null));
    insertDispatchable(dispatchable, whenFailed);

    doCommit(whenFailed);
    dispatch(dispatchable);

    interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, snapshotState._1, object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<JournalReader<? extends Entry<?>>> journalReader(final String name) {
    final JournalReader<TextEntry> reader = journalReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
      return stage().actorFor(JournalReader.class, Definition.has(JDBCJournalReaderActor.class, Definition.parameters(configuration, name)), address);
    });

    return completes().with(reader);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<StreamReader<String>> streamReader(final String name) {
    final StreamReader<String> reader = streamReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
      return stage().actorFor(StreamReader.class, Definition.has(JDBCStreamReaderActor.class, Definition.parameters(configuration)), address);
    });

    return completes().with(reader);
  }

  protected final void insertEntry(final String streamName, final int streamVersion, final Entry<String> entry, final Consumer<Exception> whenFailed) {
    try {
      final Tuple2<PreparedStatement, Optional<String>> insertEntry =
              queries.prepareInsertEntryQuery(
                      streamName,
                      streamVersion,
                      entry.entryData(),
                      entry.typeName(),
                      entry.typeVersion(),
                      gson.toJson(entry.metadata()));

      if (insertEntry._1.executeUpdate() != 1) {
        logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert event " + entry.toString());
        throw new IllegalStateException("vlingo-symbio-jdbc:journal-postrgres: Could not insert event");
      }

      if (insertEntry._2.isPresent()) {
        ((BaseEntry<String>) entry).__internal__setId(String.valueOf(insertEntry._2.get()));
      } else {
        final long id = queries.generatedKeyFrom(insertEntry._1);
        if (id > 0) {
          ((BaseEntry<String>) entry).__internal__setId(String.valueOf(id));
        }
      }
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert event " + entry.toString(), e);
      throw new IllegalStateException(e);
    }
  }

  protected final void insertSnapshot(final String streamName, final int streamVersion, final TextState snapshotState, final Consumer<Exception> whenFailed) {
    try {
      final Tuple2<PreparedStatement, Optional<String>> insertSnapshot =
              queries.prepareInsertSnapshotQuery(
                      streamName,
                      streamVersion,
                      snapshotState.data,
                      snapshotState.dataVersion,
                      snapshotState.type,
                      snapshotState.typeVersion,
                      gson.toJson(snapshotState.metadata));

      if (insertSnapshot._1.executeUpdate() != 1) {
        logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert snapshot with id " + snapshotState.id);
        throw new IllegalStateException("vlingo-symbio-jdbc:journal-postrgres: Could not insert snapshot");
      }
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert event with id " + snapshotState.id, e);
      throw new IllegalStateException(e);
    }
  }

  protected final void insertDispatchable(final Dispatchable<Entry<String>, TextState> dispatchable, final Consumer<Exception> whenFailed) {
    try {
      final String entries = dispatchable.hasEntries() ?
              dispatchable.entries().stream().map(Entry::id).collect(Collectors.joining(JDBCDispatcherControlDelegate.DISPATCHEABLE_ENTRIES_DELIMITER)) :
              "";

      final Tuple2<PreparedStatement, Optional<String>> insertDispatchable;

      final String dispatchableId = dispatchable.id();

      if (dispatchable.state().isPresent()) {
        final State<String> state = dispatchable.typedState();

        insertDispatchable =
              queries.prepareInsertDispatchableQuery(
                      dispatchableId,
                      configuration.originatorId,
                      state.id,
                      state.data,
                      state.dataVersion,
                      state.type,
                      state.typeVersion,
                      gson.toJson(state.metadata),
                      entries);
      } else {
        insertDispatchable =
                queries.prepareInsertDispatchableQuery(
                        dispatchableId,
                        configuration.originatorId,
                        null,
                        null,
                        0,
                        null,
                        0,
                        null,
                        entries);
      }

      if (insertDispatchable._1.executeUpdate() != 1) {
        logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert dispatchable with id " + dispatchable.id());
        throw new IllegalStateException("vlingo-symbio-jdbc:journal-postrgres: Could not insert snapshot");
      }
    } catch (final SQLException e) {
      whenFailed.accept(e);
      logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not insert dispatchable with id " + dispatchable.id(), e);
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
      logger().error("vlingo-symbio-jdbc:journal-postrgres: Could not complete transaction", e);
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
      logger().error("vlingo-symbio-jdbc:journal-postrgres: Cannot adapt source to entry because: ", e);
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

  private void dispatch(final Dispatchable<Entry<String>, TextState> dispatchable) {
    if (dispatcher != null) {
      //dispatch only if insert successful
      this.dispatcher.dispatch(dispatchable);
    }
  }

  private Dispatchable<Entry<String>, TextState> buildDispatchable(final String streamName, final int streamVersion, final List<Entry<String>> entries,
          final TextState snapshot) {
    final String id = getDispatchId(streamName, streamVersion);
    return new Dispatchable<>(id, LocalDateTime.now(), snapshot, entries);
  }

  private String getDispatchId(final String streamName, final int streamVersion) {
    return streamName + ":" + streamVersion + ":" + dispatchablesIdentityGenerator.generate().toString();
  }
}
