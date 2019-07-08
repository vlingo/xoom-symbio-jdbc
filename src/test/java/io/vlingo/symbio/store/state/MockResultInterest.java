// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.ReadResultInterest;
import io.vlingo.symbio.store.state.StateStore.WriteResultInterest;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MockResultInterest
    implements ReadResultInterest,
               WriteResultInterest, ConfirmDispatchedResultInterest {

  private AccessSafely access;

  private final boolean confirmDispatched;

  public AtomicInteger confirmDispatchedResultedIn = new AtomicInteger(0);
  public AtomicInteger readTextResultedIn = new AtomicInteger(0);
  public AtomicInteger writeTextResultedIn = new AtomicInteger(0);

  public AtomicReference<Result> textReadResult = new AtomicReference<>();
  public AtomicReference<Result> textWriteResult = new AtomicReference<>();
  public ConcurrentLinkedQueue<Result> textWriteAccumulatedResults = new ConcurrentLinkedQueue<>();
  public ConcurrentLinkedQueue<Source<?>> textWriteAccumulatedSources = new ConcurrentLinkedQueue<>();
  public AtomicReference<Object> stateHolder = new AtomicReference<>();
  public AtomicReference<Metadata> metadataHolder = new AtomicReference<>();
  public ConcurrentLinkedQueue<Exception> errorCauses = new ConcurrentLinkedQueue<>();

  public MockResultInterest() {
    this(true);
  }

  public MockResultInterest(final boolean confirmDispatched) {
    this.confirmDispatched = confirmDispatched;
  }

  @Override
  public void confirmDispatchedResultedIn(final Result result, final String dispatchId) {
    /*
     * update confirmDispatchedResultedIn via AccessSafely to prevent
     * database connection from closing before dispatch is confirmed
     */
    if (confirmDispatched) {
      access.writeUsing("confirmDispatchedResultedIn", 1);
    }
  }

  @Override
  public <S> void readResultedIn(final Outcome<StorageException, Result> outcome, final String id, final S state, final int stateVersion, final Metadata metadata, final Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("readStoreData", new StoreData<>(1, result, state, null, metadata, null));
        return result;
      })
      .otherwise(cause -> {
        access.writeUsing("readStoreData", new StoreData<>(1, cause.result, state, null, metadata, cause));
        return cause.result;
      });
  }

  @Override
  public <S,C> void writeResultedIn(final Outcome<StorageException, Result> outcome, final String id, final S state, final int stateVersion, final List<Source<C>> sources, final Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("writeStoreData", new StoreData<>(1, result, state, sources, null, null));
        return result;
      })
      .otherwise(cause -> {
        access.writeUsing("writeStoreData", new StoreData<>(1, cause.result, state, sources, null, cause));
        return cause.result;
      });
  }

  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely.afterCompleting(times);

    access
      .writingWith("confirmDispatchedResultedIn", (Integer increment) -> confirmDispatchedResultedIn.addAndGet(increment))
      .readingWith("confirmDispatchedResultedIn", () -> confirmDispatchedResultedIn.get())

      .writingWith("writeStoreData", (StoreData<?> data) -> {
        writeTextResultedIn.addAndGet(data.resultedIn);
        textWriteResult.set(data.result);
        textWriteAccumulatedResults.add(data.result);
        textWriteAccumulatedSources.addAll(data.sources);
        stateHolder.set(data.state);
        metadataHolder.set(data.metadata);
        if (data.errorCauses != null) {
          errorCauses.add(data.errorCauses);
        }
      })
      .writingWith("readStoreData", (StoreData<?> data) -> {
        readTextResultedIn.addAndGet(data.resultedIn);
        textReadResult.set(data.result);
        textWriteAccumulatedResults.add(data.result);
        stateHolder.set(data.state);
        metadataHolder.set(data.metadata);
        if (data.errorCauses != null) {
          errorCauses.add(data.errorCauses);
        }
      })

      .readingWith("readTextResultedIn", () -> readTextResultedIn.get())
      .readingWith("textReadResult", () -> textReadResult.get())
      .readingWith("textWriteResult", () -> textWriteResult.get())
      .readingWith("textWriteAccumulatedResults", () -> textWriteAccumulatedResults.poll())
      .readingWith("textWriteAccumulatedResultsCount", () -> textWriteAccumulatedResults.size())
      .readingWith("textWriteAccumulatedSources", () -> textWriteAccumulatedSources.poll())
      .readingWith("textWriteAccumulatedSourcesCount", () -> textWriteAccumulatedSources.size())
      .readingWith("metadataHolder", () -> metadataHolder.get())
      .readingWith("stateHolder", () -> stateHolder.get())
      .readingWith("errorCauses", () -> errorCauses.poll())
      .readingWith("errorCausesCount", () -> errorCauses.size())
      .readingWith("writeTextResultedIn", () -> writeTextResultedIn.get());

    return access;
  }


  public class StoreData<C> {
    public final Exception errorCauses;
    public final Metadata metadata;
    public final Result result;
    public final Object state;
    public final int resultedIn;
    public final List<Source<C>> sources;

    public StoreData(final int resultedIn, final Result objectResult, final Object state, final List<Source<C>> sources, final Metadata metadata, final Exception errorCauses) {
      this.resultedIn = resultedIn;
      this.result = objectResult;
      this.state = state;
      this.sources = sources;
      this.metadata = metadata;
      this.errorCauses = errorCauses;
    }
  }
}
