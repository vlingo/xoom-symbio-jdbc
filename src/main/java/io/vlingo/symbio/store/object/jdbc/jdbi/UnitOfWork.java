// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.StateObject;

/**
 * Maintains a record of the potential updates to perform
 * and whether or not those updates are necessary based on
 * modifications to the state of persistent objects, or lack
 * thereof.
 */
class UnitOfWork {
  public final Map<Long,StateObjectCopy> presistentObjects;
  public final long timestamp;
  public final long unitOfWorkId;

  /**
   * Answer a new {@code UnitOfWork} for the given {@code unitOfWorkId} and {@code stateObject}.
   * @param unitOfWorkId the long unique identity for the UnitOfWork
   * @param stateObject the Object to place under the UnitOfWork
   * @return UnitOfWork
   */
  static UnitOfWork acquireFor(final long unitOfWorkId, final Object stateObject) {
    return new UnitOfWork(unitOfWorkId, stateObject);
  }

  /**
   * Answer a new {@code UnitOfWork} for the given {@code unitOfWorkId} and {@code persistentObjects}.
   * @param unitOfWorkId the long unique identity for the UnitOfWork
   * @param persistentObjects the {@code List<?>} to place under the UnitOfWork
   * @return UnitOfWork
   */
  static UnitOfWork acquireFor(final long unitOfWorkId, final List<?> persistentObjects) {
    return new UnitOfWork(unitOfWorkId, persistentObjects);
  }

  /**
   * Constructs me state.
   * @param unitOfWorkId my long identity
   * @param stateObject the Object to manage
   */
  UnitOfWork(final long unitOfWorkId, final Object stateObject) {
    this.unitOfWorkId = unitOfWorkId;
    this.presistentObjects = StateObjectCopy.of(StateObject.from(stateObject));
    this.timestamp = System.currentTimeMillis();
  }

  UnitOfWork(final long unitOfWorkId, final List<?> persistentObjects) {
    this.unitOfWorkId = unitOfWorkId;
    this.presistentObjects = StateObjectCopy.all(persistentObjects);
    this.timestamp = System.currentTimeMillis();
  }

  /**
   * Answer whether or not I have timed out.
   * @param timeoutTime the long time marking the timeout time
   * @return boolean
   */
  boolean hasTimedOut(final long timeoutTime) {
    return timestamp <= timeoutTime;
  }

  /**
   * Answer whether or not {@code stateObject} is modified by comparing it
   * to it's {@code StateObjectCopy}.
   * @param stateObject the StateObject to compare
   * @return boolean
   */
  boolean isModified(final StateObject stateObject) {
    final StateObjectCopy original = presistentObjects.get(stateObject.persistenceId());
    return original.differsFrom(stateObject);
  }

  /**
   * A UnitOfWork appearing to be modified.
   */
  public static class AlwaysModifiedUnitOfWork extends UnitOfWork {
    AlwaysModifiedUnitOfWork() {
      super(ObjectStore.NoId, Arrays.asList());
    }

    /**
     * Answer true.
     * @return boolean.
     */
    @Override
    boolean isModified(final StateObject stateObject) {
      return true;
    }
  }
}
