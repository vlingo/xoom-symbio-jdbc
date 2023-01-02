// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.jdbc.jpa;

import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.StateObject;
/**
 * JPAObjectStore
 */
public interface JPAObjectStore extends ObjectStore {
  default <T extends StateObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest) {
    remove(persistentObject, removeId, interest, null);
  }

  <T extends StateObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest, final Object object);
}
