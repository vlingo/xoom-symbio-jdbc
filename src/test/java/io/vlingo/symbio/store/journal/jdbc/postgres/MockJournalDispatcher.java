// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

public class MockJournalDispatcher implements Dispatcher<Dispatchable<Entry<String>, State.TextState>> {
  public AccessSafely access;

  private final ConfirmDispatchedResultInterest confirmDispatchedResultInterest;
  private DispatcherControl control;
  private final Map<String,Dispatchable<Entry<String>, State.TextState>> dispatched = new HashMap<>();
  private final AtomicBoolean processDispatch = new AtomicBoolean(true);
  private final AtomicInteger dispatchAttemptCount = new AtomicInteger(0);

  public MockJournalDispatcher() {
    this.confirmDispatchedResultInterest = (result, dispatchId) -> {
      
    };
    this.access = AccessSafely.afterCompleting(0);
  }

  @Override
  public void controlWith(final DispatcherControl control) {
    this.control = control;
  }

  @Override
  public void dispatch(final Dispatchable<Entry<String>, State.TextState> dispatchable) {
    dispatchAttemptCount.getAndIncrement();
    if (processDispatch.get()) {
      final String id = dispatchable.id();
      access.writeUsing("dispatched", id, dispatchable);
      control.confirmDispatched(id, confirmDispatchedResultInterest);
    }
  }

  @SuppressWarnings({ "rawtypes" })
  public AccessSafely afterCompleting(final int times) {
    this.access = AccessSafely
      .afterCompleting(times)
      .writingWith("dispatched", (BiConsumer<String, Dispatchable>) dispatched::put)

      .writingWith("processDispatch", processDispatch::set)
      .readingWith("processDispatch", processDispatch::get)

      .readingWith("dispatchAttemptCount", dispatchAttemptCount::get)

      .readingWith("dispatched", () -> dispatched);

    return access;
  }

  public Map<String, Dispatchable<Entry<String>, State.TextState>> getDispatched() {
    return access.readFrom("dispatched");
  }
}
