// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.ReadResultInterest;
import io.vlingo.symbio.store.state.StateStore.WriteResultInterest;

public class MockResultInterest
    implements ReadResultInterest,
               WriteResultInterest,
               ConfirmDispatchedResultInterest {

  private AccessSafely access;

  public AtomicInteger confirmDispatchedResultedIn = new AtomicInteger(0);
  public AtomicInteger readTextResultedIn = new AtomicInteger(0);
  public AtomicInteger writeTextResultedIn = new AtomicInteger(0);

  public AtomicReference<Result> textReadResult = new AtomicReference<>();
  public AtomicReference<Result> textWriteResult = new AtomicReference<>();
  public ConcurrentLinkedQueue<Result> textWriteAccumulatedResults = new ConcurrentLinkedQueue<>();
  public AtomicReference<Object> stateHolder = new AtomicReference<>();
  public AtomicReference<Metadata> metadataHolder = new AtomicReference<>();
  public ConcurrentLinkedQueue<Exception> errorCauses = new ConcurrentLinkedQueue<>();

  public MockResultInterest() {
  }

  @Override
  public void confirmDispatchedResultedIn(final Result result, final String dispatchId) {
    /*
     * update confirmDispatchedResultedIn via AccessSafely to prevent
     * database connection from closing before dispatch is confirmed
     */
    System.out.println("WRITE");
    access.writeUsing("confirmDispatchedResultedIn", 1);
    System.out.println("WRITE");
  }

  @Override
  public <S> void readResultedIn(final Outcome<StorageException, Result> outcome, final String id, final S state, final int stateVersion, final Metadata metadata, final Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("readStoreData", new StoreData(1, result, state, metadata, null));
        System.out.println("WRITE");
        return result;
      })
      .otherwise(cause -> {
        access.writeUsing("readStoreData", new StoreData(1, cause.result, state, metadata, cause));
        System.out.println("WRITE");
        return cause.result;
      });
  }

  @Override
  public <S> void writeResultedIn(final Outcome<StorageException, Result> outcome, final String id, final S state, final int stateVersion, final Object object) {
    outcome
      .andThen(result -> {
        access.writeUsing("writeStoreData", new StoreData(1, result, state, null, null));
        System.out.println("WRITE");
        return result;
      })
      .otherwise(cause -> {
        access.writeUsing("writeStoreData", new StoreData(1, cause.result, state, null, cause));
        System.out.println("WRITE");
        return cause.result;
      });
  }
  
  public AccessSafely afterCompleting(final int times) {
    access = AccessSafely.afterCompleting(times);

    access
      .writingWith("confirmDispatchedResultedIn", (Integer increment) -> confirmDispatchedResultedIn.addAndGet(increment))
      .readingWith("confirmDispatchedResultedIn", () -> confirmDispatchedResultedIn.get())

      .writingWith("writeStoreData", (StoreData data) -> {
        writeTextResultedIn.addAndGet(data.resultedIn);
        textWriteResult.set(data.result);
        textWriteAccumulatedResults.add(data.result);
        stateHolder.set(data.state);
        metadataHolder.set(data.metadata);
        if (data.errorCauses != null) {
          errorCauses.add(data.errorCauses);
        }
      })
      .writingWith("readStoreData", (StoreData data) -> {
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
      .readingWith("metadataHolder", () -> metadataHolder.get())
      .readingWith("stateHolder", () -> stateHolder.get())
      .readingWith("errorCauses", () -> errorCauses.poll())
      .readingWith("errorCausesCount", () -> errorCauses.size())
      .readingWith("writeTextResultedIn", () -> writeTextResultedIn.get());

    return access;
  }


  public class StoreData {
    public final Exception errorCauses;
    public final Metadata metadata;
    public final Result result;
    public final Object state;
    public final int resultedIn;

    public StoreData(final int resultedIn, final Result objectResult, final Object state, final Metadata metadata, final Exception errorCauses) {
      this.resultedIn = resultedIn;
      this.result = objectResult;
      this.state = state;
      this.metadata = metadata;
      this.errorCauses = errorCauses;
    }
  }
}
