// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QuerySingleResult;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * JpaObjectStoreTest
 *
 */
public class JpaObjectStoreTest
{
    private JPAObjectStoreDelegate delegate;
    private ObjectStore objectStore;
    private World world;

//    @Test
    public void testThatObjectStoreInsertsOneAndQuerys()
    {
        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        persistInterest.until = TestUntil.happenings( 1 );
        Person person = new Person( 1L, 21, "Jody Jones" );
        objectStore.persist( person, person.id, persistInterest );
        persistInterest.until.completes();
        assertEquals( Result.Success, persistInterest.outcome.get().andThen( success -> success ).get() );
        
        final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
        
        queryInterest.until = TestUntil.happenings( 1 );
        MapQueryExpression expression = MapQueryExpression.using( Person.class, null, MapQueryExpression.map( "id", 1L ));
        objectStore.queryObject( expression, queryInterest );
        queryInterest.until.completes();
        assertNotNull( queryInterest.singleResult.get() );
        assertEquals( person, queryInterest.singleResult.get().persistentObject );
    }
    
    @Test
    public void testThatObjectStoreInsertsMultipleAndQueries()
    {
        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        persistInterest.until = TestUntil.happenings( 1 );
        Person person1 = new Person( 1L, 21, "Jody Jones" );
        Person person2 = new Person( 2L, 21, "Joey Jones" );
        Person person3 = new Person( 3L, 25, "Mira Jones" );
        objectStore.persistAll( Arrays.asList( person1, person2, person3), persistInterest);
        persistInterest.until.completes();
        assertEquals( Result.Success, persistInterest.outcome.get().andThen( success -> success ).get());
        
        final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
        queryInterest.multiResults.set( null );
        queryInterest.until = TestUntil.happenings( 1 );
        QueryExpression queryExpression = 
            QueryExpression.using( Person.class, "Person.allPersons" );
        objectStore.queryAll( queryExpression, queryInterest );
        queryInterest.until.completes();
        final QueryMultiResults multiResults = queryInterest.multiResults.get();
        assertNotNull( multiResults );
        assertEquals( 3, multiResults.persistentObjects.size() );
        @SuppressWarnings("unchecked")
        final Iterator<Person> iterator = (Iterator<Person>) multiResults.persistentObjects.iterator();
        assertEquals( person1, iterator.next() );
        assertEquals( person2, iterator.next() );
        assertEquals( person3, iterator.next() );
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        world = World.startWithDefaults( "jpa-test" );
        
        delegate = new JPAObjectStoreDelegate( world.stage() );
        
        objectStore = world.actorFor( ObjectStore.class, JPAObjectStoreActor.class, delegate );
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        delegate.close();
    }

    private static class TestQueryResultInterest implements QueryResultInterest {
        public AtomicReference<QueryMultiResults> multiResults = new AtomicReference<>();
        public AtomicReference<QuerySingleResult> singleResult = new AtomicReference<>();
        public TestUntil until;

        @Override
        public void queryAllResultedIn(final Outcome<StorageException, Result> outcome, final QueryMultiResults results, final Object object) {
          multiResults.set(results);
          until.happened();
        }

        @Override
        public void queryObjectResultedIn(final Outcome<StorageException, Result> outcome, final QuerySingleResult result, final Object object) {
          singleResult.set(result);
          until.happened();
        }
    }

    private static class TestPersistResultInterest implements PersistResultInterest {
        public AtomicReference<Outcome<StorageException, Result>> outcome = new AtomicReference<>();
        public TestUntil until;

        @Override
        public void persistResultedIn(
                final Outcome<StorageException, Result> outcome,
                final Object persistentObject,
                final int possible,
                final int actual,
                final Object object) {
          this.outcome.set(outcome);
          until.happened();
        }
    }
}
