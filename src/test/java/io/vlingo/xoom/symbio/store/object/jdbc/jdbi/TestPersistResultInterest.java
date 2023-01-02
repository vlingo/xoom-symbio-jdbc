// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.object.ObjectStoreWriter.PersistResultInterest;

public class TestPersistResultInterest implements PersistResultInterest {
  private AccessSafely access = AccessSafely.afterCompleting(0);
  private AtomicReference<Outcome<StorageException, Result>> outcome = new AtomicReference<>();

  @Override
  public void persistResultedIn(final Outcome<StorageException, Result> outcome, final Object persistentObject, final int possible, final int actual, final Object object) {
    this.outcome.set(outcome);
    access.writeUsing("outcome", outcome);
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely.afterCompleting(times);

    access.writingWith("outcome", (Outcome<StorageException, Result> o) -> outcome.set(o));
    access.readingWith("outcome", () -> outcome.get());

    return access;
  }
}
