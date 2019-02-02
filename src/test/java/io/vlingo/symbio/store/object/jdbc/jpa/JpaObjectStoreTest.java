/* Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved */
package io.vlingo.symbio.store.object.jdbc.jpa;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QuerySingleResult;

/**
 * JpaObjectStoreTest
 *
 * <p>Copyright (c) 2005-2019 - Blue River Systems Group, LLC - All Rights Reserved</p>
 *
 * @author mas
 * @since Feb 1, 2019
 */
public class JpaObjectStoreTest
{
    private JPAObjectStoreDelegate delegate;
    private World world;

    @Test
    public void testThatObjectStoreInsertsOneAndQuerys()
    {
        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        persistInterest.until = TestUntil.happenings( 1 );
        Person person = new Person( 1L, 10, "Jonny" );
        delegate.persist( person, person.id, persistInterest );
        persistInterest.until.completes();
        assertEquals( Result.Success, persistInterest.outcome.get().andThen( success -> success ).get() ); 
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        world = World.startWithDefaults( "jpa-test" );
        
        delegate = new JPAObjectStoreDelegate( world.stage() );
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
