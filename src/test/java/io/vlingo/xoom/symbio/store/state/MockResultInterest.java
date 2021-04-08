// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.Source;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.xoom.symbio.store.state.StateStore.ReadResultInterest;
import io.vlingo.xoom.symbio.store.state.StateStore.TypedStateBundle;
import io.vlingo.xoom.symbio.store.state.StateStore.WriteResultInterest;

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
  public CopyOnWriteArrayList<StoreData<?>> readAllStates = new CopyOnWriteArrayList<>();
  public AtomicInteger totalWrites = new AtomicInteger(0);

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
  public <S> void readResultedIn(final Outcome<StorageException, Result> outcome, final Collection<TypedStateBundle> bundles, final Object object) {
    outcome
    .andThen(result -> {
      for (final TypedStateBundle bundle : bundles) {
        access.writeUsing("readAllStates", new StoreData<>(1, result, bundle.state, Arrays.asList(), bundle.metadata, null));
      }
      return result;
    })
    .otherwise(cause -> {
      for (final TypedStateBundle bundle : bundles) {
        access.writeUsing("readAllStates", new StoreData<>(1, cause.result, bundle.state, Arrays.asList(), bundle.metadata, cause));
      }
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
        totalWrites.incrementAndGet();
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
      .writingWith("readAllStates", (StoreData<?> data) -> {
        readAllStates.add(data);
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
      .readingWith("totalWrites", () -> totalWrites.get())
      .readingWith("metadataHolder", () -> metadataHolder.get())
      .readingWith("stateHolder", () -> stateHolder.get())
      .readingWith("errorCauses", () -> errorCauses.poll())
      .readingWith("errorCausesCount", () -> errorCauses.size())
      .readingWith("writeTextResultedIn", () -> writeTextResultedIn.get())
      .readingWith("writeStoreData", () -> stateHolder.get())
      .readingWith("readStoreData", () -> stateHolder.get())
      .readingWith("readAllStates", () -> readAllStates);

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

    @SuppressWarnings("unchecked")
    public <T> T typedState() {
      return (T) state;
    }
  }
}
