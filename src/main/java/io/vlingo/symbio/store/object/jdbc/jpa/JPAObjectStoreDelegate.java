// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
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
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * The {@code JDBCObjectStoreDelegate} for JPA.
 */
public class JPAObjectStoreDelegate 
implements ObjectStore 
{
  
  private final EntityManagerFactory emf = Persistence.createEntityManagerFactory( "JpaService" );
  private final EntityManager em = emf.createEntityManager();
  private final Logger logger;

  /**
   * Constructs my default state.
   * @param configuration the Configuration used to configure my concrete subclasses
   */
  public JPAObjectStoreDelegate(final Stage stage) {
    logger = stage.world().defaultLogger();
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
   * @see io.vlingo.symbio.store.object.ObjectStore#persist(java.lang.Object, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object)
   */
  @Override
  public void persist(final Object persistentObject, final long updateId, final PersistResultInterest interest, final Object object) {
    try {
        em.getTransaction().begin();
        em.persist( persistentObject );
        em.getTransaction().commit();
        interest.persistResultedIn( Success.of( Result.Success ), persistentObject, 1, 1, object);
    } catch (Exception e)
    {
        logger.log("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e );
        em.getTransaction().rollback(); // TODO: is this necessary?
        
        interest.persistResultedIn( 
            Failure.of( new StorageException( Result.Failure, e.getMessage(), e)), 
            persistentObject, 1, 0, 
            object);
    }
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#persistAll(java.util.Collection, long, io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest, java.lang.Object)
   */
  @Override
  public void persistAll(final Collection<Object> persistentObjects, final long updateId, final PersistResultInterest interest, final Object object) {
    try
    {
        int count = 0;
        em.getTransaction().begin();
        for ( final Object o : persistentObjects )
        {
            em.persist( o );
            count++;
        }
        em.getTransaction().commit();
        interest.persistResultedIn( 
            Success.of( Result.Success ), 
            persistentObjects, persistentObjects.size(), count, 
            object);
    }
    catch ( Exception e )
    {
        logger.log( "Persist all of: " + persistentObjects + "failed because: " + e.getMessage(), e);
        
        interest.persistResultedIn(
            Failure.of( new StorageException( Result.Failure, e.getMessage(), e)), 
            persistentObjects, persistentObjects.size(), 0,
            object );
    }
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    List<?> results = null;
    
    TypedQuery<?> query = em.createNamedQuery( expression.query, expression.type );
    results = query.getResultList();
    
    interest.queryAllResultedIn(
        Success.of( Result.Success ), 
        QueryMultiResults.of( results ), 
        object);
    
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    Object obj = null;
    if ( expression.isMapQueryExpression() )
    {
        MapQueryExpression mapExpression = expression.asMapQueryExpression();
        Object idObj = mapExpression.parameters.get( "id" );
        obj = em.find( mapExpression.type, idObj );
    }
    else
    {
        // TODO: IllegalArgumentException?
        logger.log( "Unsupported query expression: " + expression.getClass().getName() );
//        interest.queryObjectResultedIn( Failure.of( Result.Failure, "" ),  null, 1, 0, object);
    }
    interest.queryObjectResultedIn( 
        Success.of( Result.Success ), 
        QuerySingleResult.of( obj ), 
        object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper)
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
}
