// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.ArrayList;
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
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStoreDelegate;
import io.vlingo.symbio.store.object.ObjectStoreReader;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.jpa.model.JPADispatchable;
import io.vlingo.symbio.store.object.jdbc.jpa.model.JPAEntry;

/**
 * The {@code JDBCObjectStoreDelegate} for JPA.
 */
public class JPAObjectStoreDelegate implements ObjectStoreDelegate<Entry<String>, State<?>> {
  // Persistence Units defined in persistence.xml
  public static final String JPA_MYSQL_PERSISTENCE_UNIT = "JpaMySqlService";
  public static final String JPA_HSQLDB_PERSISTENCE_UNIT = "JpaHsqldbService";
  public static final String JPA_POSTGRES_PERSISTENCE_UNIT = "JpaPostgresService";

  private final EntityManagerFactory emf = Persistence.createEntityManagerFactory(JPA_POSTGRES_PERSISTENCE_UNIT);
  private final EntityManager em = emf.createEntityManager();
  private final Logger logger;
  private final String originatorId;
  private final StateAdapterProvider stateAdapterProvider;

  /**
   * Constructs my default state.
   * @param originatorId the ID of {@link Dispatchable} originator
   * @param stateAdapterProvider   {@code StateAdapterProvider} used get raw {@code State<?>} from {@code PersistentObject}
   * @param logger the instance of {@link Logger} to be used
   */
  public JPAObjectStoreDelegate(final String originatorId, final StateAdapterProvider stateAdapterProvider, final Logger logger) {
    this.stateAdapterProvider = stateAdapterProvider;
    this.logger = logger;
    this.originatorId = originatorId;
    FlushModeType flushMode = em.getFlushMode();
    if (flushMode.equals(FlushModeType.AUTO))
      em.setFlushMode(FlushModeType.COMMIT);
    flushMode = em.getFlushMode();
    assert flushMode.equals(FlushModeType.COMMIT);
  }

  @Override
  public JPAObjectStoreDelegate copy() {
    return new JPAObjectStoreDelegate(this.originatorId, stateAdapterProvider, this.logger);
  }

  @Override
  public void beginTransaction() {
    em.getTransaction().begin();
  }

  @Override
  public void completeTransaction() {
    em.getTransaction().commit();
  }

  @Override
  public void failTransaction() {
    em.getTransaction().rollback(); // TODO: is this necessary?
  }

  @Override
  public <T extends PersistentObject> Collection<State<?>> persistAll(final Collection<T> persistentObjects, final long updateId, final Metadata metadata)
          throws StorageException {
    final List<State<?>> states = new ArrayList<>();
    for (final T detachedEntity : persistentObjects) {
      final State<?> state = getRawState(metadata, detachedEntity);
      createOrUpdate(detachedEntity, detachedEntity.persistenceId());
      states.add(state);
    }
    return states;
  }

  @Override
  public <T extends PersistentObject> State<?> persist(final T persistentObject, final long updateId, final Metadata metadata) throws StorageException {
    final State<?> state = getRawState(metadata, persistentObject);
    createOrUpdate(persistentObject, updateId);
    return state;
  }

  private <T extends PersistentObject> State<?> getRawState(final Metadata metadata, final T detachedEntity) {
    return this.stateAdapterProvider.asRaw(String.valueOf(detachedEntity.persistenceId()), detachedEntity, 1, metadata);
  }

  @Override
  public void persistEntries(final Collection<Entry<String>> entries) throws StorageException {
    appendEntries(entries);
  }

  @Override
  public void persistDispatchable(final Dispatchable<Entry<String>, State<?>> dispatchable) throws StorageException {
    em.persist(JPADispatchable.fromDispatchable(originatorId, dispatchable));
  }

  @Override
  public QueryMultiResults queryAll(final QueryExpression expression) throws StorageException {

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
    final List<?> results = query.getResultList();
    em.getTransaction().commit();

    return new ObjectStoreReader.QueryMultiResults(results);
  }

  @Override
  public QuerySingleResult queryObject(final QueryExpression expression) throws StorageException {
    final Object obj;
    if (expression.isMapQueryExpression()) {
      final MapQueryExpression mapExpression = expression.asMapQueryExpression();
      final Object idObj = mapExpression.parameters.get("id");
      obj = findEntity(mapExpression.type, idObj);
      if (obj != null)
        em.detach(obj);
    } else {
      throw new StorageException(Result.Error, "Unsupported query expression: " + expression.getClass().getName());
    }
    return QuerySingleResult.of(obj);
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


  public <T extends PersistentObject> int remove(final T persistentObject, final long removeId) {
    try {
      int count = 0;
      final PersistentObject managedEntity = (PersistentObject) findEntity(persistentObject.getClass(), removeId);
      if (managedEntity != null) {
        em.getTransaction().begin();
        em.remove(managedEntity);
        em.getTransaction().commit();
        count++;
      } else {
        throw new StorageException(Result.NotFound, "Could not find " + persistentObject + " with id=" +removeId);
      }
      return count;
    } catch (final Exception e) {
      em.getTransaction().rollback();
      final String errorMsg = "Removal of: " + persistentObject + " failed because: " + e.getMessage();
      logger.error(errorMsg, e);

      throw new StorageException(Result.Error, errorMsg, e);
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public Collection<Dispatchable<Entry<String>, State<?>>> allUnconfirmedDispatchableStates() {
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
    beginTransaction();
    try {
      em.createNamedQuery("Dispatchables.deleteByDispatchId")
              .setParameter(1, dispatchId)
              .executeUpdate();
      completeTransaction();
    } catch (final Exception e){
      logger.error("Failed to confirm dispatch id {}", dispatchId, e);
      failTransaction();
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

  private Object findEntity(final Class<?> entityClass, final Object primaryKey) {
    return em.find(entityClass, primaryKey);
  }

  /**
   * @param detachedEntity unmanaged entity to be persisted if does not exist or merged if does exist.
   * @param updateId the primary key to be used to find the managed entity.
   */
  private void createOrUpdate(final Object detachedEntity, final long updateId) {
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
  private void appendEntries(final Collection<Entry<String>> entries) {
    for (final Entry<String> entry : entries) {
      final JPAEntry jpaEntry;
      if (entry instanceof JPAEntry) {
        jpaEntry = (JPAEntry) entry;
        em.persist(entry);
      } else {
        jpaEntry = new JPAEntry(entry);
        em.persist(jpaEntry);
        ((BaseEntry<?>) entry).__internal__setId(jpaEntry.id());
        logger.debug("BASEENTRY COPY: " + entry);
      }
      logger.debug("em.persist(" + jpaEntry + ")");
    }
  }

}
