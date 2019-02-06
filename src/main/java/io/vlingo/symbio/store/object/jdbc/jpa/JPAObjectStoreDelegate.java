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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import io.vlingo.actors.Logger;
import io.vlingo.actors.Stage;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

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

  /**
   * Constructs my default state.
   * 
   * @param stage
   */
  public JPAObjectStoreDelegate(final Stage stage) {
    logger = stage.world().defaultLogger();
    FlushModeType flushMode = em.getFlushMode();
    if (flushMode.equals(FlushModeType.AUTO))
      em.setFlushMode(FlushModeType.COMMIT);
    flushMode = em.getFlushMode();
    assert flushMode.equals(FlushModeType.COMMIT);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    try {
      em.close();
    } catch (Exception e) {
      logger.log("Close failed because: " + e.getMessage(), e);
    }
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object,
   * long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void persist(final Object persistentObject, final long updateId, final PersistResultInterest interest,
          final Object object) {
    try {
      em.getTransaction().begin();
      createOrUpdate(persistentObject, updateId);
      em.getTransaction().commit();
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, 1, object);
    } catch (Exception e) {
      logger.log("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);
      em.getTransaction().rollback(); // TODO: is this necessary?

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection,
   * long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void persistAll(final Collection<Object> persistentObjects, final long updateId,
          final PersistResultInterest interest, final Object object) {
    try {
      int count = 0;
      em.getTransaction().begin();
      for (final Object o : persistentObjects) {
        PersistentObject po = (PersistentObject) o;
        createOrUpdate(po, po.persistenceId());
        count++;
      }
      em.getTransaction().commit();
      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), count,
              object);
    } catch (Exception e) {
      logger.log("Persist all of: " + persistentObjects + "failed because: " + e.getMessage(), e);

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
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    List<?> results = null;

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
    results = query.getResultList();
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
    Object obj = null;
    if (expression.isMapQueryExpression()) {
      MapQueryExpression mapExpression = expression.asMapQueryExpression();
      Object idObj = mapExpression.parameters.get("id");
      obj = findObject(mapExpression.type, idObj);
      if (obj != null)
        em.detach(obj);
    } else {
      // TODO: IllegalArgumentException?
      logger.log("Unsupported query expression: " + expression.getClass().getName());
      // interest.queryObjectResultedIn( Failure.of( Result.Failure, "" ), null, 1, 0,
      // object);
    }
    interest.queryObjectResultedIn(Success.of(Result.Success), QuerySingleResult.of(obj), object);
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStore#remove(java.lang.
   * Object, long,
   * io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest,
   * java.lang.Object)
   */
  @Override
  public void remove(Object persistentObject, long removeId, PersistResultInterest interest, Object object) {
    try {
      int count = 0;
      Object managedObject = findObject(persistentObject.getClass(), removeId);
      if (managedObject != null) {
        em.getTransaction().begin();
        em.remove(managedObject);
        em.getTransaction().commit();
        count++;
      }
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, count, object);
    } catch (Exception e) {
      logger.log("Removal of: " + persistentObject + " failed because: " + e.getMessage(), e);

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
    // TODO: implementation
  }

  /**
   * Check for timed out transaction
   */
  public void timeoutCheck() {
    // TODO: implementation
  }

  protected Object findObject(Class<?> entityClass, Object primaryKey) {
    return em.find(entityClass, primaryKey);
  }

  /**
   * @param persistentObject
   */
  protected void createOrUpdate(final Object persistentObject, final long updateId) {
    if (ObjectStore.isNoId(updateId)) {
      /*
       * RDB is expected to provide the id in this case.
       */
      em.persist(persistentObject);
    } else {
      Object managedObject = findObject(persistentObject.getClass(), updateId);
      if (managedObject == null) {
        /*
         * App provided id is not yet saved.
         */
        em.persist(persistentObject);
      } else {
        /*
         * object to be updated
         */
        em.merge(persistentObject);
      }
    }
  }

}
