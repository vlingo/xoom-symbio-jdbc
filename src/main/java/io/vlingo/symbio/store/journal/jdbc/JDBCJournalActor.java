// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.*;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalReaderActor.JDBCJournalReaderInstantiator;
import io.vlingo.symbio.store.journal.jdbc.JDBCStreamReaderActor.JDBCStreamReaderInstantiator;

import java.util.*;
import java.util.function.Consumer;

public class JDBCJournalActor extends Actor implements Journal<String> {
    private final JDBCJournalWriter journalWriter;
    private final EntryAdapterProvider entryAdapterProvider;
    private final StateAdapterProvider stateAdapterProvider;
    private final Configuration configuration;
    private final DatabaseType databaseType;
    private final Map<String, JournalReader<TextEntry>> journalReaders;
    private final Map<String, StreamReader<String>> streamReaders;

    public JDBCJournalActor(final JDBCJournalWriter journalWriter, final Configuration configuration) {
        this.journalWriter = journalWriter;
        this.configuration = configuration;
        this.databaseType = configuration.databaseType;
        this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
        this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
        this.journalReaders = new HashMap<>();
        this.streamReaders = new HashMap<>();
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
            logger().error("vlingo-symbio-jdbc:journal-" + databaseType + ": Cannot adapt source to entry because: ", e);
            throw new IllegalArgumentException(e);
        }
    }

    private <ST> Optional<TextState> toState(final String streamName, final ST snapshot, final int streamVersion) {
        return snapshot == null
                ? Optional.empty()
                : Optional.of(stateAdapterProvider.asRaw(streamName, snapshot, streamVersion));
    }
}
