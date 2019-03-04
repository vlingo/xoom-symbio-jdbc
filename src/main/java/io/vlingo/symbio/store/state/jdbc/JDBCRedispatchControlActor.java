// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.jdbc;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;

import io.vlingo.actors.Actor;
import io.vlingo.common.Cancellable;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.Dispatchable;
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore.RedispatchControl;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
/**
 * JDBCRedispatchControlActor is responsible for requesting re-dispatch
 * of the unconfirmed dispatchables of a JDBCStateStoreActor on a
 * configurable, periodic basis. This allows the work of re-dispatching
 * to be shifted to a different thread than the one responsible for
 * reading and writing in the state store.
 */
public class JDBCRedispatchControlActor extends Actor
implements DispatcherControl, RedispatchControl, Scheduled<Object> {
  
  public final static long DEFAULT_REDISPATCH_DELAY = 2000L;

  private final StorageDelegate delegate;
  private final Dispatcher dispatcher;
  private final long confirmationExpiration;
  private final Cancellable cancellable;
  
  @SuppressWarnings("unchecked")
  public JDBCRedispatchControlActor(final Dispatcher dispatcher, final StorageDelegate delegate, final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.dispatcher = dispatcher;
    this.delegate = delegate;
    this.confirmationExpiration = confirmationExpiration;
    this.cancellable = scheduler().schedule(selfAs(Scheduled.class), null, DEFAULT_REDISPATCH_DELAY, checkConfirmationExpirationInterval);
    System.out.println("JDBCRedispatchControlActor constructed at " + System.currentTimeMillis());
  }
  
  @Override
  public void intervalSignal(final Scheduled<Object> scheduled, final Object data) {
    System.out.println("JDBCRedispatchControlActor::intervalSignal at " + System.currentTimeMillis());
    dispatchUnconfirmed();
  }
  
  @Override
  public void confirmDispatched(String dispatchId, ConfirmDispatchedResultInterest interest) {
    checkConnection();
    try {
      delegate.confirmDispatched(dispatchId);
      interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
    }
    catch (Exception ex) {
      logger().log(getClass().getSimpleName() + " confirmDispatched() failed because: " + ex.getMessage(), ex);
    }
  }
  
  @Override
  public void dispatchUnconfirmed() {
    checkConnection();
    try {
      final LocalDateTime now = LocalDateTime.now();
      Collection<Dispatchable<TextState>> all = delegate.allUnconfirmedDispatchableStates();
      for (final Dispatchable<TextState> dispatchable : all) {
        final LocalDateTime then = dispatchable.createdAt;
        Duration duration = Duration.between(then, now);
        if (Math.abs(duration.toMillis()) > confirmationExpiration) {
          dispatcher.dispatch(dispatchable.id, dispatchable.state.asTextState());
        }
      }
    }
    catch (Exception e) {
      logger().log(getClass().getSimpleName() + " dispatchUnconfirmed() failed because: " + e.getMessage(), e);
    }
  }
  
  @Override
  public void stop() {
    cancel();
    super.stop();
  }
  
  private void cancel() {
    if (cancellable != null) {
      cancellable.cancel();
    }
  }

  private void checkConnection() {
    if (delegate.isClosed()) {
      cancel();
    }
  }
}
