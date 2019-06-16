// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.actors.Logger;
import io.vlingo.actors.Stage;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStoreReader;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The {@code JDBCObjectStoreDelegate} for JPA.
 */
public class JPAObjectStoreDelegate implements JPAObjectStore {
  // Persistence Units defined in persistence.xml
  public static final String JPA_MYSQL_PERSISTENCE_UNIT = "JpaMySqlService";
  public static final String JPA_HSQLDB_PERSISTENCE_UNIT = "JpaHsqldbService";
  public static final String JPA_POSTGRES_PERSISTENCE_UNIT = "JpaPostgresService";

  private final EntityManagerFactory emf = Persistence.createEntityManagerFactory(JPA_POSTGRES_PERSISTENCE_UNIT);
  private final EntityManager em = emf.createEntityManager();
  private final Logger logger;
  private final EntryAdapterProvider entryAdapterProvider;


  /**
   * Constructs my default state.
   *
   * @param stage from which to obtain the default logger.
   */
  public JPAObjectStoreDelegate(final Stage stage) {
    logger = stage.world().defaultLogger();
    FlushModeType flushMode = em.getFlushMode();
    if (flushMode.equals(FlushModeType.AUTO))
      em.setFlushMode(FlushModeType.COMMIT);
    flushMode = em.getFlushMode();
    assert flushMode.equals(FlushModeType.COMMIT);
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage.world());
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    try {
      em.close();
    } catch (Exception e) {
      logger.error("Close failed because: " + e.getMessage(), e);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persist(T persistentObject, List<Source<E>> sources, long updateId, PersistResultInterest interest, Object object) {
    try {
      em.getTransaction().begin();
      createOrUpdate(persistentObject, updateId);
      appendSources(sources);
      em.getTransaction().commit();
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, 1, object);
    } catch (Exception e) {
      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);
      em.getTransaction().rollback(); // TODO: is this necessary?

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, java.util.List, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object) */
  @Override
  public <T extends PersistentObject, E> void persistAll(Collection<T> persistentObjects, List<Source<E>> sources, long updateId, PersistResultInterest interest, Object object) {
    try {
      int count = 0;
      em.getTransaction().begin();
      for (final T detachedEntity : persistentObjects) {
        createOrUpdate(detachedEntity, detachedEntity.persistenceId());
        count++;
      }
      appendSources(sources);
      em.getTransaction().commit();
      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), count,
              object);
    } catch (Exception e) {
      logger.error("Persist all of: " + persistentObjects + "failed because: " + e.getMessage(), e);

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObjects,
              persistentObjects.size(), 0, object);
    }
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
      List<?> parameters = expression.asListQueryExpression().parameters;
      if (parameters != null) {
        for (int i = 0; i < parameters.size(); i++) {
          int parmOrdinal = i + 1;
          query = query.setParameter(parmOrdinal, parameters.get(i));
        }
      }
    } else if (expression.isMapQueryExpression()) {
      Map<String, ?> parameters = expression.asMapQueryExpression().parameters;
      for (String key : parameters.keySet()) {
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
      MapQueryExpression mapExpression = expression.asMapQueryExpression();
      Object idObj = mapExpression.parameters.get("id");
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
  public <T extends PersistentObject> void remove(T persistentObject, long removeId, PersistResultInterest interest, Object object) {
    try {
      int count = 0;
      PersistentObject managedEntity = (PersistentObject) findEntity(persistentObject.getClass(), removeId);
      if (managedEntity != null) {
        em.getTransaction().begin();
        em.remove(managedEntity);
        em.getTransaction().commit();
        count++;
      }
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, count, object);
    } catch (Exception e) {
      logger.error("Removal of: " + persistentObject + " failed because: " + e.getMessage(), e);

      em.getTransaction().rollback();

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.
   * store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(PersistentObjectMapper mapper) {
    throw new UnsupportedOperationException("registerMapper is unnecessary for JPA.");
  }

  protected Object findEntity(Class<?> entityClass, Object primaryKey) {
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
      Object managedEntity = findEntity(detachedEntity.getClass(), updateId);
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
  private <E> void appendSources(List<Source<E>> sources) {
    final Collection<Entry<String>> entries = entryAdapterProvider.asEntries(sources);
    for (Entry<String> entry : entries) {
      JPAEntry jpaEntry = entry instanceof JPAEntry
        ? (JPAEntry) entry
        : new JPAEntry(entry);
      em.persist(jpaEntry);
      logger.debug("em.persist(" + jpaEntry + ")");
    }
  }
}
