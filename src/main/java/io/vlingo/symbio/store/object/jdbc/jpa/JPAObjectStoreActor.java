// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;

import io.vlingo.actors.Actor;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * JPAObjectStoreActor
 *
 */
public class JPAObjectStoreActor extends Actor implements JPAObjectStore, Scheduled {
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

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object,
   * long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void persist(Object persistentObject, long updateId, PersistResultInterest interest, Object object) {
    delegate.persist(persistentObject, updateId, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection,
   * long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void persistAll(Collection<Object> persistentObjects, long updateId, PersistResultInterest interest,
          Object object) {
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
  public void queryAll(QueryExpression expression, QueryResultInterest interest, Object object) {
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
  public void queryObject(QueryExpression expression, QueryResultInterest interest, Object object) {
    delegate.queryObject(expression, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.
   * store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(PersistentObjectMapper mapper) {
    delegate.registerMapper(mapper);
  }

  /*
   * @see io.vlingo.common.Scheduled#intervalSignal(io.vlingo.common.Scheduled,
   * java.lang.Object)
   */
  @Override
  public void intervalSignal(Scheduled scheduled, Object data) {
    delegate.timeoutCheck();
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStore#remove(java.lang.
   * Object, long,
   * io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void remove(Object persistentObject, long removeId, PersistResultInterest interest, Object object) {
    delegate.remove(persistentObject, removeId, interest);
  }

}
