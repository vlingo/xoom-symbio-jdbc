// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.symbio.store.object.ObjectStore;

/**
 * JPAObjectStore
 *
 */
public interface JPAObjectStore extends ObjectStore {
  default void remove(final Object persistentObject, final long removeId, final PersistResultInterest interest) {
    remove(persistentObject, removeId, interest, null);
  }

  void remove(final Object persistentObject, final long removeId, final PersistResultInterest interest,
          final Object object);
}
