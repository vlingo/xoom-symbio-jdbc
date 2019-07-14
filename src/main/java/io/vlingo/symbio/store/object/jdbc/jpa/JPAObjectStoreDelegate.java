// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import io.vlingo.actors.Logger;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStoreReader;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.jpa.model.JPADispatchable;
import io.vlingo.symbio.store.object.jdbc.jpa.model.JPAEntry;

/**
 * The {@code JDBCObjectStoreDelegate} for JPA.
 */
public class JPAObjectStoreDelegate implements JPAObjectStore, DispatcherControl.DispatcherControlDelegate<Entry<?>, State<?>>{
  // Persistence Units defined in persistence.xml
  public static final String JPA_MYSQL_PERSISTENCE_UNIT = "JpaMySqlService";
  public static final String JPA_HSQLDB_PERSISTENCE_UNIT = "JpaHsqldbService";
  public static final String JPA_POSTGRES_PERSISTENCE_UNIT = "JpaPostgresService";

  private final EntityManagerFactory emf = Persistence.createEntityManagerFactory(JPA_POSTGRES_PERSISTENCE_UNIT);
  private final EntityManager em = emf.createEntityManager();
  private final Logger logger;
  private final String originatorId;

  /**
   * Constructs my default state.
   *
   * @param originatorId the ID of {@link Dispatchable} originator
   * @param logger the instance of {@link Logger} to be used
   */
  public JPAObjectStoreDelegate(final String originatorId, final Logger logger) {
    this.logger = logger;
    this.originatorId = originatorId;
    FlushModeType flushMode = em.getFlushMode();
    if (flushMode.equals(FlushModeType.AUTO))
      em.setFlushMode(FlushModeType.COMMIT);
    flushMode = em.getFlushMode();
    assert flushMode.equals(FlushModeType.COMMIT);
  }

  public JPAObjectStoreDelegate copy() {
    return new JPAObjectStoreDelegate(this.originatorId, this.logger);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    try {
      em.close();
    } catch (final Exception e) {
      logger.error("Close failed because: " + e.getMessage(), e);
    }
  }


  public final void beginWrite(){
    em.getTransaction().begin();
  }

  public final void complete(){
    em.getTransaction().commit();
  }

  public final void fail(){
    em.getTransaction().rollback(); // TODO: is this necessary?
  }

  public final <T extends PersistentObject> void persist(final T persistentObject, final long updateId, final List<Entry<String>> entries,
          final Dispatchable<Entry<String>, State<?>> dispatchable){
    createOrUpdate(persistentObject, updateId);
    appendEntries(entries);
    appendDispatchable(dispatchable);
  }

  public final <T extends PersistentObject> void persistAll(final Collection<T> persistentObjects, final long updateId,  final List<Entry<String>> entries,
          final Collection<Dispatchable<Entry<String>, State<?>>> dispatchables){
    for (final T detachedEntity : persistentObjects) {
      createOrUpdate(detachedEntity, detachedEntity.persistenceId());
    }
    appendEntries(entries);

    for (final Dispatchable<Entry<String>, State<?>> stateDispatchable : dispatchables) {
      appendDispatchable(stateDispatchable);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final Metadata metadata, final long updateId,
          final PersistResultInterest interest, final Object object) {
    //not to be used
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {
    //not to be used
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.
   * object.QueryExpression,
   * io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest,
   * java.lang.Object)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    List<? extends PersistentObject> results = null;

    TypedQuery<?> query = em.createNamedQuery(expression.query, expression.type);

    if (expression.isListQueryExpression()) {
      final List<?> parameters = expression.asListQueryExpression().parameters;
      if (parameters != null) {
        for (int i = 0; i < parameters.size(); i++) {
          final int parmOrdinal = i + 1;
          query = query.setParameter(parmOrdinal, parameters.get(i));
        }
      }
    } else if (expression.isMapQueryExpression()) {
      final Map<String, ?> parameters = expression.asMapQueryExpression().parameters;
      for (final String key : parameters.keySet()) {
        query = query.setParameter(key, parameters.get(key));
      }
    }

    em.getTransaction().begin();
    results = (List<? extends PersistentObject>) query.getResultList();
    em.getTransaction().commit();

    interest.queryAllResultedIn(Success.of(Result.Success), QueryMultiResults.of(results), object);

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
    PersistentObject obj = null;
    if (expression.isMapQueryExpression()) {
      final MapQueryExpression mapExpression = expression.asMapQueryExpression();
      final Object idObj = mapExpression.parameters.get("id");
      obj = (PersistentObject) findEntity(mapExpression.type, idObj);
      if (obj != null)
        em.detach(obj);
    } else {
      // TODO: IllegalArgumentException?
      logger.error("Unsupported query expression: " + expression.getClass().getName());
      // interest.queryObjectResultedIn( Failure.of( Result.Failure, "" ), null, 1, 0,
      // object);
    }
    interest.queryObjectResultedIn(Success.of(Result.Success), QuerySingleResult.of(obj), object);
  }

  @Override
  public <T extends PersistentObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest, final Object object) {
    try {
      int count = 0;
      final PersistentObject managedEntity = (PersistentObject) findEntity(persistentObject.getClass(), removeId);
      if (managedEntity != null) {
        em.getTransaction().begin();
        em.remove(managedEntity);
        em.getTransaction().commit();
        count++;
      }
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, count, object);
    } catch (final Exception e) {
      logger.error("Removal of: " + persistentObject + " failed because: " + e.getMessage(), e);

      em.getTransaction().rollback();

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public List<Dispatchable<Entry<?>, State<?>>> allUnconfirmedDispatchableStates() throws Exception {
    return em.createNamedQuery("Dispatchables.getUnconfirmed", JPADispatchable.class)
            .setParameter("orignatorId", originatorId)
            .getResultStream()
            .map(JPADispatchable::toDispatchable)
            .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   */  @Override
  public void confirmDispatched(final String dispatchId) {
    beginWrite();
    try {
      em.createNamedQuery("Dispatchables.deleteByDispatchId")
              .setParameter(1, dispatchId)
              .executeUpdate();
      complete();
    } catch (final Exception e){
      logger.error("Failed to confirm dispatch id {}", dispatchId, e);
      fail();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
     this.close();
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.
   * store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    throw new UnsupportedOperationException("registerMapper is unnecessary for JPA.");
  }

  protected Object findEntity(final Class<?> entityClass, final Object primaryKey) {
    return em.find(entityClass, primaryKey);
  }

  /**
   * @param detachedEntity unmanaged entity to be persisted if does not exist or merged if does exist.
   * @param updateId the primary key to be used to find the managed entity.
   */
  protected void createOrUpdate(final Object detachedEntity, final long updateId) {
    if (ObjectStoreReader.isNoId(updateId)) {
      /*
       * RDB is expected to provide the id in this case.
       */
      em.persist(detachedEntity);
    } else {
      final Object managedEntity = findEntity(detachedEntity.getClass(), updateId);
      if (managedEntity == null) {
        /*
         * App provided id is not yet saved.
         */
        em.persist(detachedEntity);
      } else {
        /*
         * object to be updated
         */
        em.merge(detachedEntity);
      }
    }
  }

  /**
   * Convert each {@link Source} in {@code sources} to a {@link
   */
  private <E> void appendEntries(final Collection<Entry<String>> entries) {
    for (final Entry<String> entry : entries) {
      final JPAEntry jpaEntry = entry instanceof JPAEntry
        ? (JPAEntry) entry
        : new JPAEntry(entry);
      em.persist(jpaEntry);
      ((BaseEntry<?>) entry).__internal__setId(jpaEntry.id());
      logger.debug("em.persist(" + jpaEntry + ")");
    }
  }

  private void appendDispatchable(final Dispatchable<Entry<String>, State<?>> stateDispatchable) {
     em.persist(JPADispatchable.fromDispatchable(originatorId, stateDispatchable));
  }
}
