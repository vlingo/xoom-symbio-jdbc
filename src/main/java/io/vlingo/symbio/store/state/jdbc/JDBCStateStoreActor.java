// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;

public class JDBCStateStoreActor extends Actor implements StateStore {
  private final StorageDelegate delegate;
  private final Dispatcher dispatcher;
  private final DispatcherControl dispatcherControl;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;

  public JDBCStateStoreActor(final Dispatcher dispatcher, final StorageDelegate delegate) {
    this(dispatcher, delegate, 1000L, 1000L);
  }

  public JDBCStateStoreActor(final Dispatcher dispatcher, final StorageDelegate delegate, final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.dispatcher = dispatcher;
    this.delegate = delegate;

    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());

    this.dispatcherControl = stage().actorFor(
      DispatcherControl.class,
      Definition.has(
        JDBCDispatcherControlActor.class,
        Definition.parameters(dispatcher, delegate, checkConfirmationExpirationInterval, confirmationExpiration))
    );

    dispatcher.controlWith(dispatcherControl);
    dispatcherControl.dispatchUnconfirmed();
  }

  @Override
  public void stop() {
    if (dispatcherControl != null) {
      dispatcherControl.stop();
    }
    super.stop();
  }

  @Override
  public void read(final String id, Class<?> type, final ReadResultInterest interest, final Object object) {
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
          if (result.first()) {
            final TextState raw = delegate.stateFrom(result, id);
            final Object state = stateAdapterProvider.fromRaw(raw);
            interest.readResultedIn(Success.of(Result.Success), id, state, raw.dataVersion, raw.metadata, object);
          } else {
            interest.readResultedIn(Failure.of(new StorageException(Result.NotFound, "Not found for: " + id)), id, null, -1, null, object);
          }
        }
        delegate.complete();
      } catch (Exception e) {
        delegate.fail();
        interest.readResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, null, -1, null, object);
        logger().log(
                getClass().getSimpleName() +
                " readText() failed because: " + e.getMessage() +
                " for: " + (id == null ? "unknown id" : id),
                e);
      }
    } else {
      logger().log(
              getClass().getSimpleName() +
              " readText() missing ResultInterest for: " +
              (id == null ? "unknown id" : id));
    }
  }

  @Override
  public <S,C> void write(final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Metadata metadata, final WriteResultInterest interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.Error, "The state is null.")), id, state, stateVersion, sources, object);
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

          // TODO: Write sources

          delegate.beginWrite();
          final PreparedStatement writeStatement = delegate.writeExpressionFor(storeName, raw);
          writeStatement.execute();
          final String dispatchId = storeName + ":" + id;
          final PreparedStatement dispatchableStatement = delegate.dispatchableWriteExpressionFor(dispatchId, raw);
          dispatchableStatement.execute();
          delegate.complete();
          dispatch(dispatchId, raw);

          interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, sources, object);
        } catch (Exception e) {
          logger().log(getClass().getSimpleName() + " writeText() error because: " + e.getMessage(), e);
          delegate.fail();
          interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), id, state, stateVersion, sources, object);
        }
      }
    } else {
      logger().log(
              getClass().getSimpleName() +
              " writeText() missing ResultInterest for: " +
              (state == null ? "unknown id" : id));
    }
  }

  private <C> void appendEntries(final List<Source<C>> sources) {
    final Collection<Entry<?>> all = entryAdapterProvider.asEntries(sources);
    // TODO: entries.addAll(all);
  }

  private void dispatch(final String dispatchId, final State<String> state) {
    dispatcher.dispatch(dispatchId, state.asTextState());
  }
}
