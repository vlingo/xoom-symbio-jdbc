// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.actors.Definition;
import io.vlingo.common.*;
import io.vlingo.reactivestreams.Stream;
import io.vlingo.symbio.*;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.QueryExpression;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStoreEntryReader;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class JDBCStateStoreActor extends Actor implements StateStore, Scheduled<Object> {
  private final JDBCStorageDelegate<TextState> delegate;
  private final JDBCEntriesWriter entriesWriter;
  private final Map<String,StateStoreEntryReader<?>> entryReaders;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final ReadAllResultCollector readAllResultCollector;

  public JDBCStateStoreActor(final JDBCStorageDelegate<TextState> delegate, final JDBCEntriesInstantWriter entriesWriter) {
    this.delegate = delegate;
    this.entriesWriter = entriesWriter;
    this.entriesWriter.setLogger(logger());
    this.entryReaders = new HashMap<>();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.readAllResultCollector = new ReadAllResultCollector();
  }

  public JDBCStateStoreActor(final JDBCStorageDelegate<TextState> delegate, final JDBCEntriesBatchWriter entriesWriter, int timeBetweenFlushWrites) {
    this.delegate = delegate;
    this.entriesWriter = entriesWriter;
    this.entriesWriter.setLogger(logger());
    this.entryReaders = new HashMap<>();

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.readAllResultCollector = new ReadAllResultCollector();

    stage().scheduler().schedule(selfAs(Scheduled.class), null, 5, timeBetweenFlushWrites);
  }

  @Override
  public void stop() {
    for (final StateStoreEntryReader<?> reader : entryReaders.values()) {
      reader.close();
    }
    delegate.close();
    entriesWriter.stop();
    super.stop();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <ET extends Entry<?>> Completes<StateStoreEntryReader<ET>> entryReader(final String name) {
    StateStoreEntryReader<?> reader = entryReaders.get(name);
    if (reader == null) {
      final EntryReader.Advice advice = delegate.entryReaderAdvice();
      final ActorInstantiator<?> instantiator = delegate.instantiator();
      instantiator.set("advice", advice);
      instantiator.set("name", name);
      reader = childActorFor(StateStoreEntryReader.class, Definition.has(advice.entryReaderClass, instantiator));
      entryReaders.put(name, reader);
    }
    return completes().with((StateStoreEntryReader<ET>) reader);
  }

  @Override
  public void read(final String id, final Class<?> type, final ReadResultInterest interest, final Object object) {
    if (interest != null) {
      if (id == null || type == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.Error, id == null ? "The id is null." : "The type is null.")), id, null, -1, null, object);
        return;
      }

      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);

      if (storeName == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), id, null, -1, null, object);
        return;
      }

      try {
        delegate.beginRead();
        final PreparedStatement readStatement = delegate.readExpressionFor(storeName, id);
        try (final ResultSet result = readStatement.executeQuery()) {
          final TextState raw = delegate.stateFrom(result, id);
          if (!raw.isEmpty()) {
            final Object state = stateAdapterProvider.fromRaw(raw);
            interest.readResultedIn(Success.of(Result.Success), id, state, raw.dataVersion, raw.metadata, object);
          } else {
            interest.readResultedIn(Failure.of(new StorageException(Result.NotFound, "Not found for: " + id)), id, null, -1, null, object);
          }
        }
        delegate.complete();
      } catch (final Exception e) {
        delegate.fail();
        interest.readResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, null, -1, null, object);
        logger().error(
                getClass().getSimpleName() +
                " readText() failed because: " + e.getMessage() +
                " for: " + (id == null ? "unknown id" : id),
                e);
      }
    } else {
      logger().warn(
              getClass().getSimpleName() +
              " readText() missing ResultInterest for: " +
              (id == null ? "unknown id" : id));
    }
  }

  @Override
  public void readAll(final Collection<TypedStateBundle> bundles, final ReadResultInterest interest, final Object object) {
    readAllResultCollector.prepare();

    for (final TypedStateBundle bundle : bundles) {
      read(bundle.id, bundle.type, readAllResultCollector, null);
    }

    final Outcome<StorageException, Result> outcome = readAllResultCollector.readResultOutcome(bundles.size());

    interest.readResultedIn(outcome, readAllResultCollector.readResultBundles(), object);
  }

  @Override
  public Completes<Stream> streamAllOf(final Class<?> type) {
    final String storeName = StateTypeStateStoreMap.storeNameFrom(type);

    PreparedStatement readStatement = null;

    try {
      delegate.beginRead();
      readStatement = delegate.readAllExpressionFor(storeName);
      final ResultSet resultSet = readStatement.executeQuery();
      delegate.complete();
      return completes().with(new JDBCStateStoreStream<>(stage(), delegate, stateAdapterProvider, resultSet, logger()));
    } catch (final Exception e) {
      delegate.fail();
      logger().error(
              getClass().getSimpleName() +
              " streamAllOf() failed because: " + e.getMessage() +
              " for: " + storeName,
              e);
    }

    return completes().with(null); // this should be an EmptyStream
  }

  @Override
  public Completes<Stream> streamSomeUsing(final QueryExpression query) {
    final String storeName = StateTypeStateStoreMap.storeNameFrom(query.type);

    PreparedStatement readSomeStatement = null;

    try {
      readSomeStatement = delegate.readSomeExpressionFor(storeName, query);
      delegate.beginRead();
      final ResultSet resultSet = readSomeStatement.executeQuery();
      delegate.complete();
      return completes().with(new JDBCStateStoreStream<>(stage(), delegate, stateAdapterProvider, resultSet, logger()));
    } catch (Exception e) {
      delegate.fail();
      logger().error(
              getClass().getSimpleName() +
              " streamSomeUsing() failed because: " + e.getMessage() +
              " for: " + storeName,
              e);
    }

    return completes().with(null); // this should be an EmptyStream
  }

  @Override
  public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata,
          final WriteResultInterest interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.Error, "The state is null.")), id,null, stateVersion, sources, object);
      } else {
        try {
          final String storeName = StateTypeStateStoreMap.storeNameFrom(state.getClass());

          if (storeName == null) {
            interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), id, state, stateVersion, sources, object);
            return;
          }

          final TextState raw = metadata == null ?
                  stateAdapterProvider.asRaw(id, state, stateVersion) :
                  stateAdapterProvider.asRaw(id, state, stateVersion, metadata);

          final List<Entry<?>> entries = buildEntries(sources, stateVersion, metadata);
          entriesWriter.appendEntries(storeName, entries, raw, outcome -> interest.writeResultedIn(outcome, id, state, stateVersion, sources, object));
        } catch (final Exception e) {
          logger().error(getClass().getSimpleName() + " writeText() error because: " + e.getMessage(), e);
          interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, sources, object);
        }
      }
    } else {
      logger().warn(
              getClass().getSimpleName() +
                      " writeText() missing ResultInterest for: " +
                      (state == null ? "unknown id" : id));
    }
  }

  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object o) {
    entriesWriter.flush();
  }

  private <C> List<Entry<?>> buildEntries(final List<Source<C>> sources, final int stateVersion, final Metadata metadata) {
    if (sources.isEmpty()) return Collections.emptyList();

    try {
      return entryAdapterProvider.asEntries(sources, stateVersion, metadata);
    } catch (final Exception e) {
      final String message = "Failed to adapt entry because: " + e.getMessage();
      logger().error(message, e);
      throw new IllegalStateException(message, e);
    }
  }
}
