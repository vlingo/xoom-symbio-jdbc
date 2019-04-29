// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.ListQueryExpression;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEntryAdapters.PersonAddedAdapter;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEntryAdapters.PersonRenamedAdapter;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEvents.PersonAdded;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEvents.PersonRenamed;
/**
 * JPAObjectStoreIntegrationTest is responsible for testing {@link JPAObjectStore}
 */
public class JPAObjectStoreIntegrationTest {
  private JPAObjectStoreDelegate delegate;
  private JPAObjectStore objectStore;
  private World world;

  @Test
  public void testThatObjectStoreInsertsOneAndQuerys() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
    Person person = new Person(1L, 21, "Jody Jones");
    objectStore.persist(person, Arrays.asList(new PersonAdded(person)), person.persistenceId(), persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result")); 

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);

    MapQueryExpression expression = MapQueryExpression.using(Person.class, null, MapQueryExpression.map("id", 1L));
    objectStore.queryObject(expression, queryInterest);
    assertNotNull( queryInterestAccess.readFrom("singleResult")); 
    assertEquals(person, queryInterestAccess.readFrom("singleResultValue")); 

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    AccessSafely removeInterestAccess = removeInterest.afterCompleting(1);
    objectStore.remove(person, person.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQueries() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");
    objectStore.persistAll(
      Arrays.asList(person1, person2, person3),
      Arrays.asList(new PersonAdded(person1), new PersonAdded(person2), new PersonAdded(person3)),
      persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);
    queryInterest.multiResults.set(null);
    QueryExpression queryExpression = QueryExpression.using(Person.class, "Person.allPersons");
    objectStore.queryAll(queryExpression, queryInterest);
    assertNotNull(queryInterestAccess.readFrom("multiResults"));
    assertEquals(3, (int)queryInterestAccess.readFrom("multiResultsSize")); 
    assertEquals(person1, queryInterestAccess.readFrom("multiResultsValue", 0)); 
    assertEquals(person2, queryInterestAccess.readFrom("multiResultsValue", 1)); 
    assertEquals(person3, queryInterestAccess.readFrom("multiResultsValue", 2));

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    AccessSafely removeInterestAccess = removeInterest.afterCompleting(3);
    objectStore.remove(person1, person1.persistenceId(), removeInterest);
    objectStore.remove(person2, person2.persistenceId(), removeInterest);
    objectStore.remove(person3, person3.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
    assertEquals(3, (int)removeInterestAccess.readFrom("count"));
  }

  @Test
  public void testThatSingleEntityUpdates() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
    Person person1 = new Person(1L, 21, "Jody Jones");

    objectStore.persist(person1, Arrays.asList(new PersonAdded(person1)), person1.persistenceId(), persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

    final TestPersistResultInterest updateInterest = new TestPersistResultInterest();
    final AccessSafely updateInterestAccess = updateInterest.afterCompleting(1);
    person1 = person1.newPersonWithName( person1.name + " " + person1.persistenceId() );
    objectStore.persist(person1, Arrays.asList(new PersonRenamed(person1)), person1.persistenceId(), updateInterest);
    assertEquals(Result.Success, updateInterestAccess.readFrom("result"));

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    final AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);
    MapQueryExpression expression = MapQueryExpression.using(Person.class, null, MapQueryExpression.map("id", 1L));
    objectStore.queryObject(expression, queryInterest);
    assertNotNull( queryInterestAccess.readFrom("singleResult"));
    Person queriedPerson = (Person) queryInterestAccess.readFrom("singleResultValue");
    assertEquals(person1.persistenceId(), queriedPerson.persistenceId());
    assertEquals(person1.age, queriedPerson.age);
    assertEquals(person1.name, queriedPerson.name);

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    final AccessSafely removeInterestAccess = removeInterest.afterCompleting(1);
    objectStore.remove(person1, person1.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
  }

  @Test
  public void testThatMultipleEntitiesUpdate() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(
      Arrays.asList(person1, person2, person3),
      Arrays.asList(new PersonAdded(person1), new PersonAdded(person2), new PersonAdded(person3)),
      persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result")); 

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    final AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);

    queryInterest.multiResults.set(null);
    QueryExpression queryExpression = QueryExpression.using(Person.class, "Person.allPersons");
    objectStore.queryAll(queryExpression, queryInterest);

    assertNotNull(queryInterestAccess.readFrom("multiResultsValue"));
    int count = queryInterestAccess.readFrom("multiResultsSize");
    assertEquals(3, count);

    final List<Person> modifiedPersons = new ArrayList<>();
    for ( int i = 0; i < count; i++ ) {
      Person person = queryInterestAccess.readFrom("multiResultsValue", i);
      person = person.newPersonWithName(person.name + " " + person.persistenceId());
      modifiedPersons.add(person);
    }

    final TestPersistResultInterest updateInterest = new TestPersistResultInterest();
    final AccessSafely updateInterestAccess = updateInterest.afterCompleting(1);
    objectStore.persistAll(
      modifiedPersons,
      Arrays.asList(new PersonRenamed(person1), new PersonRenamed(person2), new PersonRenamed(person3)),
      updateInterest);
    assertEquals(Result.Success, updateInterestAccess.readFrom("result"));

    final TestQueryResultInterest queryInterest2 = new TestQueryResultInterest();
    final AccessSafely queryInterest2Access = queryInterest2.afterCompleting(1);
    queryInterest.multiResults.set(null);
    objectStore.queryAll(queryExpression, queryInterest2);

    assertEquals(3, (int)queryInterest2Access.readFrom("multiResultsSize"));
    for (int i = 0; i < 3; i++) {
      Person modifiedPerson = (Person) modifiedPersons.get(i);
      Person queriedPerson = (Person) queryInterest2Access.readFrom("multiResultsValue", i);
      assertEquals(modifiedPerson.persistenceId(), queriedPerson.persistenceId());
      assertEquals(modifiedPerson.age, queriedPerson.age);
      assertEquals(modifiedPerson.name, queriedPerson.name);
    }

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    final AccessSafely removeInterestAccess = removeInterest.afterCompleting(3);
    objectStore.remove(person1, person1.persistenceId(), removeInterest);
    objectStore.remove(person2, person2.persistenceId(), removeInterest);
    objectStore.remove(person3, person3.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
    assertEquals(3, (int)removeInterestAccess.readFrom("count"));
  }

  @Test
  public void testMultipleEntitiesParameterizedListQuery() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(
      Arrays.asList(person1, person2, person3),
      Arrays.asList(new PersonAdded(person1), new PersonAdded(person2), new PersonAdded(person3)),
      persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    final AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);
    queryInterest.multiResults.set(null);
    ListQueryExpression queryExpression = ListQueryExpression.using(Person.class, "Person.adultsParmList",
            Arrays.asList(22));
    objectStore.queryAll(queryExpression, queryInterest);
    assertNotNull(queryInterestAccess.readFrom("multiResults"));
    assertEquals(1, (int)queryInterestAccess.readFrom("multiResultsSize"));
    assertEquals(person3, queryInterestAccess.readFrom("multiResultsValue", 0));

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    final AccessSafely removeInterestAccess = removeInterest.afterCompleting(3);
    objectStore.remove(person1, person1.persistenceId(), removeInterest);
    objectStore.remove(person2, person2.persistenceId(), removeInterest);
    objectStore.remove(person3, person3.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
    assertEquals(3, (int)removeInterestAccess.readFrom("count"));
  }

  @Test
  public void testMultipleEntitiesParameterizedMapQuery() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(
      Arrays.asList(person1, person2, person3),
      Arrays.asList(new PersonAdded(person1), new PersonAdded(person2), new PersonAdded(person3)),
      persistInterest);
    assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    final AccessSafely queryInterestAccess = queryInterest.afterCompleting(1);
    queryInterest.multiResults.set(null);
    Map<String, Integer> parmMap = Collections.singletonMap("age", 22);
    MapQueryExpression queryExpression = MapQueryExpression.using(Person.class, "Person.adultsParmMap", parmMap);
    objectStore.queryAll(queryExpression, queryInterest);
    assertNotNull(queryInterestAccess.readFrom("multiResults"));
    assertEquals(1, (int)queryInterestAccess.readFrom("multiResultsSize"));
    assertEquals(person3, queryInterestAccess.readFrom("multiResultsValue", 0));

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    final AccessSafely removeInterestAccess = removeInterest.afterCompleting(3);
    objectStore.remove(person1, person1.persistenceId(), removeInterest);
    objectStore.remove(person2, person2.persistenceId(), removeInterest);
    objectStore.remove(person3, person3.persistenceId(), removeInterest);
    assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
    assertEquals(3, (int)removeInterestAccess.readFrom("count"));
  }
  
  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    world = World.startWithDefaults("jpa-test");
    delegate = new JPAObjectStoreDelegate(world.stage());
    objectStore = world.actorFor(JPAObjectStore.class, JPAObjectStoreActor.class, delegate);
    EntryAdapterProvider.instance(world).registerAdapter(PersonAdded.class, new PersonAddedAdapter());
    EntryAdapterProvider.instance(world).registerAdapter(PersonRenamed.class, new PersonRenamedAdapter());    
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    objectStore.close();
  }

  private static class TestQueryResultInterest implements QueryResultInterest {
    public AtomicReference<QueryMultiResults> multiResults = new AtomicReference<>();
    public AtomicReference<QuerySingleResult> singleResult = new AtomicReference<>();
    private AccessSafely access = AccessSafely.afterCompleting(1);

    @Override
    public void queryAllResultedIn(final Outcome<StorageException, Result> outcome, final QueryMultiResults results, final Object object) {
      access.writeUsing( "addAll", results );
    }

    @Override
    public void queryObjectResultedIn(final Outcome<StorageException, Result> outcome, final QuerySingleResult result, final Object object) {
      access.writeUsing( "add", result );
    }
    
    public AccessSafely afterCompleting( final int times ) {
      access =
        AccessSafely
          .afterCompleting( times )
          .writingWith("add", (value)-> singleResult.set((QuerySingleResult)value))
          .writingWith("addAll", (values) -> multiResults.set((QueryMultiResults)values))
          .readingWith("singleResult", () -> singleResult.get())
          .readingWith("singleResultValue", () -> singleResult.get().persistentObject)
          .readingWith("multiResults", () -> multiResults.get())
          .readingWith("multiResultsValue", () -> multiResults.get().persistentObjects)
          .readingWith("multiResultsSize", () -> multiResults.get().persistentObjects().size())
          .readingWith("multiResultsValue", (index) -> {
            Person person = null;
            if ( multiResults.get().persistentObjects.size() > (int)index )
            {
              person = (Person)multiResults.get().persistentObjects.toArray()[(int)index];
            }
            return person;
          })
          .readingWith("multiResultsValueAsArray", () -> multiResults.get().persistentObjects.toArray());
      return access;
    }
  }

  private static class TestPersistResultInterest implements PersistResultInterest {
    public AtomicReference<Outcome<StorageException, Result>> outcome = new AtomicReference<>();
    public AtomicInteger count = new AtomicInteger(0);
    private AccessSafely access = AccessSafely.afterCompleting(1);

    @Override
    public void persistResultedIn(Outcome<StorageException, Result> outcome, Object persistentObject, int possible, int actual, Object object) {
      access.writeUsing("set", outcome);
    }

    @SuppressWarnings("unchecked")
    public AccessSafely afterCompleting( final int times ) {
      access =
        AccessSafely
          .afterCompleting(times)
          .writingWith("set", (value) -> {
            outcome.set((Outcome<StorageException,Result>)value);
            count.getAndAdd(1);
          })
          .readingWith("result", () -> outcome.get().andThen(success -> success).get())
          .readingWith("count", () -> count.get());
      return access;
    }
  }
}
