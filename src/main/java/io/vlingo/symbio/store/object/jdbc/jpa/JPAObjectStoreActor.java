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
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
/**
 * JPAObjectStoreActor
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

  @Override
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persist(persistentObject, sources, updateId, interest, object);
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persistAll(persistentObjects, sources, updateId, interest, object);
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

  @Override
  public <T extends PersistentObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest, final Object object) {
    delegate.remove(persistentObject, removeId, interest);
  }
}
