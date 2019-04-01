// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapter;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * JPAObjectStoreActor
 *
 */
public class JPAObjectStoreActor extends Actor implements JPAObjectStore {
  private boolean closed;
  private final JPAObjectStoreDelegate delegate;

  public JPAObjectStoreActor(final JPAObjectStoreDelegate delegate) {
    this.delegate = delegate;
    this.closed = false;
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#close() */
  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      this.closed = true;
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object, java.util.List, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
  @Override
  public <E> void persist(final Object persistentObject, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persist(persistentObject, updateId, interest, object);
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, java.util.List, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
  @Override
  public <E> void persistAll(final Collection<Object> persistentObjects, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persistAll(persistentObjects, updateId, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.
   * object.QueryExpression,
   * io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest,
   * java.lang.Object)
   */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryAll(expression, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.
   * object.QueryExpression,
   * io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest,
   * java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryObject(expression, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.
   * store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    delegate.registerMapper(mapper);
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStore#remove(java.lang.
   * Object, long,
   * io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void remove(final Object persistentObject, final long removeId, final PersistResultInterest interest, final Object object) {
    delegate.remove(persistentObject, removeId, interest);
  }

}
