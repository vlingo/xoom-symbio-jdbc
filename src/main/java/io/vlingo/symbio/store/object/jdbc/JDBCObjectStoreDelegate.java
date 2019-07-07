// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;

import java.util.Collection;
import java.util.List;

/**
 * The {@code JDBCObjectStoreDelegate} abstract base used by
 * {@code JDBCObjectStoreActor} to interact with specific delegates,
 * and also extended by any number of those concrete delegates.
 */
public abstract class JDBCObjectStoreDelegate implements ObjectStore, DispatcherControl.DispatcherControlDelegate<Entry<?>, State<?>> {
  public final Configuration configuration;

  /**
   * Constructs my default state.
   *
   * @param configuration the Configuration used to configure my concrete subclasses
   */
  protected JDBCObjectStoreDelegate(final Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Check for timed out transactions.
   */
  public abstract void timeoutCheck();
  
  public abstract <T extends PersistentObject> void persist(final T persistentObject, final long updateId,  List<BaseEntry.TextEntry> entries, Dispatchable<BaseEntry.TextEntry, State.TextState> dispatchable);

  public abstract <T extends PersistentObject> void persistAll(final Collection<T> persistentObjects, final long updateId,  List<BaseEntry.TextEntry> entries, Collection<Dispatchable<BaseEntry.TextEntry, State.TextState>> dispatchables);

  public abstract void beginWrite();

  public abstract void complete();

  public abstract void fail();

  public abstract JDBCObjectStoreDelegate copy() ;
}
