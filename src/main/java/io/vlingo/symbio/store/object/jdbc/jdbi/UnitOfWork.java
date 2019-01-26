// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
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
import io.vlingo.symbio.store.object.PersistentObject;

/**
 * Maintains a record of the potential updates to perform
 * and whether or not those updates are necessary based on
 * modifications to the state of persistent objects, or lack
 * thereof.
 */
class UnitOfWork {
  public final Map<Long,PersistentObjectCopy> presistentObjects;
  public final long timestamp;
  public final long unitOfWorkId;

  /**
   * Answer a new {@code UnitOfWork} for the given {@code unitOfWorkId} and {@code persistentObject}.
   * @param unitOfWorkId the long unique identity for the UnitOfWork
   * @param persistentObject the Object to place under the UnitOfWork
   * @return UnitOfWork
   */
  static UnitOfWork acquireFor(final long unitOfWorkId, final Object persistentObject) {
    return new UnitOfWork(unitOfWorkId, persistentObject);
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
   * @param persistentObject the Object to manage
   */
  UnitOfWork(final long unitOfWorkId, final Object persistentObject) {
    this.unitOfWorkId = unitOfWorkId;
    this.presistentObjects = PersistentObjectCopy.of(PersistentObject.from(persistentObject));
    this.timestamp = System.currentTimeMillis();
  }

  UnitOfWork(final long unitOfWorkId, final List<?> persistentObjects) {
    this.unitOfWorkId = unitOfWorkId;
    this.presistentObjects = PersistentObjectCopy.all(persistentObjects);
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
   * Answer whether or not {@code persistentObject} is modified by comparing it
   * to it's {@code PersistentObjectCopy}.
   * @param persistentObject the PersistentObject to compare
   * @return boolean
   */
  boolean isModified(final PersistentObject persistentObject) {
    final PersistentObjectCopy original = presistentObjects.get(persistentObject.persistenceId());
    return original.differsFrom(persistentObject);
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
    boolean isModified(final PersistentObject persistentObject) {
      return true;
    }
  }
}
