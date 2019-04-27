// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * The actor implementing the {@code ObjectStore} protocol in behalf of
 * any number of {@code JDBCObjectStoreDelegate} types.
 */
public class JDBCObjectStoreActor extends Actor implements ObjectStore, Scheduled<Object> {
  private boolean closed;
  private final JDBCObjectStoreDelegate delegate;

  @SuppressWarnings("unchecked")
  public JDBCObjectStoreActor(final JDBCObjectStoreDelegate delegate) {
    this.delegate = delegate;
    this.closed = false;

    final long timeout = delegate.configuration.transactionTimeoutMillis;
    stage().scheduler().schedule(selfAs(Scheduled.class), null, timeout, timeout);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      closed = true;
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStoreWriter#persist(io.vlingo.symbio.store.object.PersistentObject, java.util.List, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object) */
  @Override
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persist(persistentObject, updateId, interest, object);
  }

  /* @see io.vlingo.symbio.store.object.ObjectStoreWriter#persistAll(java.util.Collection, java.util.List, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object) */
  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    delegate.persistAll(persistentObjects, updateId, interest, object);    
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryAll(expression, interest, object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryObject(expression, interest, object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(java.lang.Object)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    delegate.registerMapper(mapper);
  }

  @Override
  public void intervalSignal(final Scheduled<Object> scheduled, final Object data) {
    delegate.timeoutCheck();
  }

  /*
   * @see io.vlingo.actors.Actor#stop()
   */
  @Override
  public void stop() {
    close();

    super.stop();
  }
}
