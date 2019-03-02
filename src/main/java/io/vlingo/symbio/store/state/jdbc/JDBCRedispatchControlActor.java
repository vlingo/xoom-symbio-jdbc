// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.jdbc;

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
  
  private final StorageDelegate delegate;
  private final Dispatcher dispatcher;
  private final Cancellable cancellable;
  
  public JDBCRedispatchControlActor(final Dispatcher dispatcher, final StorageDelegate delegate, final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.dispatcher = dispatcher;
    this.delegate = delegate;
    this.cancellable = scheduler().schedule(selfAs(Scheduled.class), null, confirmationExpiration, checkConfirmationExpirationInterval);
  }
  
  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object data) {
    dispatchUnconfirmed();
  }
  
  @Override
  public void confirmDispatched(String dispatchId, ConfirmDispatchedResultInterest interest) {
    if (delegate.isClosed()) return;
    try {
      delegate.confirmDispatched(dispatchId);
      interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
    }
    catch (Exception ex) {
      logger().log(getClass().getSimpleName() + " confirmDispatched() failed because: " + ex.getMessage(), ex);
      cancel();
    }
  }
  
  @Override
  public void dispatchUnconfirmed() {
    if (delegate.isClosed()) return;
    try {
      Collection<Dispatchable<TextState>> all = delegate.allUnconfirmedDispatchableStates();
      for (final Dispatchable<TextState> dispatchable : all) {
        dispatcher.dispatch(dispatchable.id, dispatchable.state.asTextState());
      }
    }
    catch (Exception e) {
      logger().log(getClass().getSimpleName() + " dispatchUnconfirmed() failed because: " + e.getMessage(), e);
      cancel();
    }
  }
  
  /* @see io.vlingo.actors.Actor#stop() */
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
}
