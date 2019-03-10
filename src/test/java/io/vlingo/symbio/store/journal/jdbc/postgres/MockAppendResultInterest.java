// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.journal.Journal.AppendResultInterest;

public class MockAppendResultInterest implements AppendResultInterest {
  private AccessSafely access = afterCompleting(0);
  private AtomicInteger failureCount = new AtomicInteger(0);
  private AtomicInteger successCount = new AtomicInteger(0);

  @Override
  public <S,ST> void appendResultedIn(Outcome<StorageException, Result> outcome, String streamName, int streamVersion,
          Source<S> source, Optional<ST> snapshot, Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("successCount", 1);
        return result;
      })
      .otherwise(failure -> {
        access.writeUsing("failureCount", 1);
        return failure.result;
      });
  }

  @Override
  public <S,ST> void appendAllResultedIn(Outcome<StorageException, Result> outcome, String streamName, int streamVersion,
          List<Source<S>> sources, Optional<ST> snapshot, Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("successCount", 1);
        return result;
      })
      .otherwise(failure -> {
        access.writeUsing("failureCount", 1);
        return failure.result;
      });
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely.afterCompleting(times);

    access
      .writingWith("failureCount", (Integer increment) -> failureCount.addAndGet(increment))
      .readingWith("failureCount", () -> failureCount.get())
      .writingWith("successCount", (Integer increment) -> successCount.addAndGet(increment))
      .readingWith("successCount", () -> successCount.get());

    return access;
  }
}
