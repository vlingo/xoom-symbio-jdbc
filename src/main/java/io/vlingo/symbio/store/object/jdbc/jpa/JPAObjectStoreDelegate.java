// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;

import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreDelegate;

/**
 * The {@code JDBCObjectStoreDelegate} for JPA.
 */
public class JPAObjectStoreDelegate extends JDBCObjectStoreDelegate {

  /**
   * Constructs my default state.
   * @param configuration the Configuration used to configure my concrete subclasses
   */
  public JPAObjectStoreDelegate(final Configuration configuration) {
    super(configuration);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object)
   */
  @Override
  public void persist(final Object persistentObject, final long updateId, final PersistResultInterest interest, final Object object) {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object)
   */
  @Override
  public void persistAll(final Collection<Object> persistentObjects, final long updateId, final PersistResultInterest interest, final Object object) {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(PersistentObjectMapper mapper) {
    // TODO: implementation
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreDelegate#timeoutCheck()
   */
  @Override
  public void timeoutCheck() {
    // TODO: implementation
  }
}
