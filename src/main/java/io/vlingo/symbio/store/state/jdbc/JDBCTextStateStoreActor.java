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

import io.vlingo.actors.Actor;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.TextStateStore;

public class JDBCTextStateStoreActor extends Actor implements TextStateStore, DispatcherControl {
  private final static TextState EmptyState = TextState.Null;

  private final StorageDelegate delegate;
  private final Dispatcher dispatcher;

  public JDBCTextStateStoreActor(final Dispatcher dispatcher, final StorageDelegate delegate) {
    this.dispatcher = dispatcher;
    this.delegate = delegate;

    final DispatcherControl control = selfAs(DispatcherControl.class);
    dispatcher.controlWith(control);
    control.dispatchUnconfirmed();
  }

  @Override
  public void confirmDispatched(final String dispatchId, final ConfirmDispatchedResultInterest interest) {
    delegate.confirmDispatched(dispatchId);
    interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
  }

  @Override
  public void dispatchUnconfirmed() {
    try {
      Collection<Dispatchable<TextState>> all = delegate.allUnconfirmedDispatchableStates();
      for (final Dispatchable<TextState> dispatchable : all) {
        dispatch(dispatchable.id, dispatchable.state);
      }
    } catch (Exception e) {
      logger().log(getClass().getSimpleName() + " dispatchUnconfirmed() failed because: " + e.getMessage(), e);
    }
  }

  @Override
  public void read(final String id, Class<?> type, final ReadResultInterest<TextState> interest) {
    read(id, type, interest, null);
  }

  @Override
  public void read(final String id, Class<?> type, final ReadResultInterest<TextState> interest, final Object object) {
    if (interest != null) {
      if (id == null || type == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.Error, id == null ? "The id is null." : "The type is null.")), id, EmptyState, object);
        return;
      }

      final String storeName = StateTypeStateStoreMap.storeNameFrom(type);

      if (storeName == null) {
        interest.readResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), id, EmptyState, object);
        return;
      }

      try {
        delegate.beginRead();
        final PreparedStatement readStatement = delegate.readExpressionFor(storeName, id);
        try (final ResultSet result = readStatement.executeQuery()) {
          if (result.first()) {
            final TextState state = delegate.stateFrom(result, id);
            interest.readResultedIn(Success.of(Result.Success), id, state, object);
          } else {
            interest.readResultedIn(Failure.of(new StorageException(Result.NotFound, "Not found for: " + id)), id, EmptyState, object);
          }
        }
        delegate.complete();
      } catch (Exception e) {
        delegate.fail();
        interest.readResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, EmptyState, object);
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
  public void write(final TextState state, final WriteResultInterest<TextState> interest) {
    write(state, interest, null);
  }

  @Override
  public void write(final TextState state, final WriteResultInterest<TextState> interest, final Object object) {
    if (interest != null) {
      if (state == null) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.Error, "The state is null.")), null, EmptyState, object);
      } else {
        try {
          final String storeName = StateTypeStateStoreMap.storeNameFrom(state.type);

          if (storeName == null) {
            interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, "No type store.")), state.id, state, object);
            return;
          }

          delegate.beginWrite();
          final PreparedStatement writeStatement = delegate.writeExpressionFor(storeName, state);
          writeStatement.execute();
          final String dispatchId = storeName + ":" + state.id;
          final PreparedStatement dispatchableStatement = delegate.dispatchableWriteExpressionFor(dispatchId, state);
          dispatchableStatement.execute();
          delegate.complete();
          dispatch(dispatchId, state);

          interest.writeResultedIn(Success.of(Result.Success), state.id, state, object);
        } catch (Exception e) {
          logger().log(getClass().getSimpleName() + " writeText() error because: " + e.getMessage(), e);
          delegate.fail();
          interest.writeResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), state.id, state, object);
        }
      }
    } else {
      logger().log(
              getClass().getSimpleName() +
              " writeText() missing ResultInterest for: " +
              (state == null ? "unknown id" : state.id));
    }
  }

  private void dispatch(final String dispatchId, final State<String> state) {
    dispatcher.dispatch(dispatchId, state.asTextState());
  }
}
