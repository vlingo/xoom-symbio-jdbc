// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.actors.Address;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.common.Scheduled;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.*;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.journal.Journal;
import io.vlingo.xoom.symbio.store.journal.JournalReader;
import io.vlingo.xoom.symbio.store.journal.StreamReader;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCJournalReaderActor.JDBCJournalReaderInstantiator;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCStreamReaderActor.JDBCStreamReaderInstantiator;

import java.sql.Connection;
import java.util.*;
import java.util.function.Consumer;

public class JDBCJournalActor extends Actor implements Journal<String>, Scheduled<Object> {
  private final JDBCJournalWriter journalWriter;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final Configuration configuration;
  private final DatabaseType databaseType;
  private final Map<String, JournalReader<TextEntry>> journalReaders;
  private final Map<String, StreamReader<String>> streamReaders;

  private JDBCJournalActor(final Configuration configuration, final JDBCJournalWriter journalWriter, Object object) throws Exception {
    this.journalWriter = journalWriter;
    this.configuration = configuration;
    this.databaseType = configuration.databaseType;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.journalReaders = new HashMap<>();
    this.streamReaders = new HashMap<>();

    try (Connection initConnection = configuration.connectionProvider.newConnection()) {
      try {
        JDBCQueries queries = JDBCQueries.queriesFor(initConnection);
        queries.createTables(initConnection);
        initConnection.commit();
      } catch (Exception e) {
        initConnection.rollback();
        throw new IllegalStateException("Failed to initialize JDBCJournalActor because: " + e.getMessage(), e);
      }
    }
  }

  public JDBCJournalActor(final Configuration configuration, final JDBCJournalInstantWriter journalWriter) throws Exception {
    this(configuration, journalWriter, null);
  }

  @SuppressWarnings("unchecked")
  public JDBCJournalActor(final Configuration configuration, final JDBCJournalBatchWriter journalWriter, int timeBetweenFlushWrites) throws Exception {
    this(configuration, journalWriter, null);

    stage().scheduler().schedule(selfAs(Scheduled.class), null, 5, timeBetweenFlushWrites);
  }

  @Override
  public void stop() {
    journalWriter.stop();
    super.stop();
  }

  @Override
  public <S, ST> void append(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata,
                             final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = ex -> appendResultedInFailure(streamName, streamVersion, source, null, interest, object, ex);
    final Entry<String> entry = asEntry(source, streamVersion, metadata, whenFailed);
    final Consumer<Outcome<StorageException, Result>> postAppendAction = outcome -> interest.appendResultedIn(outcome, streamName, streamVersion, source,
        Optional.empty(), object);
    journalWriter.appendEntry(streamName, streamVersion, entry, Optional.empty(), postAppendAction);
  }

  @Override
  public <S, ST> void appendWith(final String streamName, final int streamVersion, final Source<S> source, final Metadata metadata, final ST snapshot,
                                 final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = ex -> appendResultedInFailure(streamName, streamVersion, source, snapshot, interest, object, ex);
    final Entry<String> entry = asEntry(source, streamVersion, metadata, whenFailed);
    final Optional<TextState> snapshotState = toState(streamName, snapshot, streamVersion);
    final Consumer<Outcome<StorageException, Result>> postAppendAction = outcome -> interest.appendResultedIn(outcome, streamName, streamVersion, source,
        Optional.ofNullable(snapshot), object);
    journalWriter.appendEntry(streamName, streamVersion, entry, snapshotState, postAppendAction);
  }

  @Override
  public <S, ST> void appendAll(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final Metadata metadata,
                                final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = e -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, null, interest, object, e);
    final List<Entry<String>> entries = asEntries(sources, fromStreamVersion, metadata, whenFailed);
    final Consumer<Outcome<StorageException, Result>> postAppendAction = outcome -> interest.appendAllResultedIn(outcome, streamName, fromStreamVersion, sources,
        Optional.empty(), object);
    journalWriter.appendEntries(streamName, fromStreamVersion, entries, Optional.empty(), postAppendAction);
  }

  @Override
  public <S, ST> void appendAllWith(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final Metadata metadata,
                                    final ST snapshot, final AppendResultInterest interest, final Object object) {
    final Consumer<Exception> whenFailed = e -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, snapshot, interest, object, e);
    final List<Entry<String>> entries = asEntries(sources, fromStreamVersion, metadata, whenFailed);
    final Optional<TextState> snapshotState = toState(streamName, snapshot, fromStreamVersion);
    final Consumer<Outcome<StorageException, Result>> postAppendAction = outcome -> interest.appendAllResultedIn(outcome, streamName, fromStreamVersion, sources,
        Optional.ofNullable(snapshot), object);
    journalWriter.appendEntries(streamName, fromStreamVersion, entries, snapshotState, postAppendAction);
  }

  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object o) {
    journalWriter.flush();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<JournalReader<? extends Entry<?>>> journalReader(final String name) {
    final JournalReader<TextEntry> reader = journalReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
      return stage().actorFor(JournalReader.class, Definition.has(JDBCJournalReaderActor.class, new JDBCJournalReaderInstantiator(configuration, name)), address);
    });

    return completes().with(reader);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<StreamReader<String>> streamReader(final String name) {
    final StreamReader<String> reader = streamReaders.computeIfAbsent(name, (key) -> {
      final Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
      return stage().actorFor(StreamReader.class, Definition.has(JDBCStreamReaderActor.class, new JDBCStreamReaderInstantiator(configuration)), address);
    });

    return completes().with(reader);
  }

  private <S, ST> void appendResultedInFailure(final String streamName, final int streamVersion, final Source<S> source, final ST snapshot,
                                               final AppendResultInterest interest, final Object object, final Exception ex) {
    interest.appendResultedIn(Failure.of(new StorageException(Result.Failure, ex.getMessage(), ex)), streamName, streamVersion, source,
        Optional.ofNullable(snapshot), object);
  }

  private <S, ST> void appendAllResultedInFailure(final String streamName, final int streamVersion, final List<Source<S>> sources, final ST snapshot,
                                                  final AppendResultInterest interest, final Object object, final Exception e) {
    interest.appendAllResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), streamName, streamVersion, sources,
        snapshot == null ? Optional.empty() : Optional.of(snapshot), object);
  }

  private <S> List<Entry<String>> asEntries(final List<Source<S>> sources, final int fromStreamVersion, final Metadata metadata, final Consumer<Exception> whenFailed) {
    final List<Entry<String>> entries = new ArrayList<>(sources.size());
    int version = fromStreamVersion;
    for (final Source<?> source : sources) {
      entries.add(asEntry(source, version++, metadata, whenFailed));
    }
    return entries;
  }

  private <S> Entry<String> asEntry(final Source<S> source, final int streamVersion, final Metadata metadata, final Consumer<Exception> whenFailed) {
    try {
      return entryAdapterProvider.asEntry(source, streamVersion, metadata);
    } catch (final Exception e) {
      whenFailed.accept(e);
      logger().error("xoom-symbio-jdbc:journal-" + databaseType + ": Cannot adapt source to entry because: ", e);
      throw new IllegalArgumentException(e);
    }
  }

  private <ST> Optional<TextState> toState(final String streamName, final ST snapshot, final int streamVersion) {
    return snapshot == null
        ? Optional.empty()
        : Optional.of(stateAdapterProvider.asRaw(streamName, snapshot, streamVersion));
  }

  public static class JDBCJournalActorInstantiator implements ActorInstantiator<JDBCJournalActor> {
    private static final long serialVersionUID = 2184177499416088762L;

    private final Configuration configuration;
    private final JDBCJournalWriter journalWriter;
    private final Optional<Integer> timeBetweenFlushWrites;

    public JDBCJournalActorInstantiator(final Configuration configuration, final JDBCJournalInstantWriter journalWriter) {
      this.configuration = configuration;
      this.journalWriter = journalWriter;
      this.timeBetweenFlushWrites = Optional.empty();
    }

    public JDBCJournalActorInstantiator(final Configuration configuration, final JDBCJournalBatchWriter journalWriter, int timeBetweenFlushWrites) {
      this.configuration = configuration;
      this.journalWriter = journalWriter;
      this.timeBetweenFlushWrites = Optional.of(timeBetweenFlushWrites);
    }

    @Override
    public JDBCJournalActor instantiate() {
      JDBCJournalActor instance;
      try {
        if (timeBetweenFlushWrites.isPresent()) {
          int time = timeBetweenFlushWrites.get();
          instance = new JDBCJournalActor(configuration, (JDBCJournalBatchWriter) journalWriter, time);
        } else {
          instance = new JDBCJournalActor(configuration, (JDBCJournalInstantWriter) journalWriter);
        }
      } catch (Exception e) {
        throw new IllegalStateException("Could not instantiate JDBCJournalActor because: " + e.getMessage(), e);
      }

      journalWriter.setLogger(instance.logger());

      return instance;
    }
  }
}
