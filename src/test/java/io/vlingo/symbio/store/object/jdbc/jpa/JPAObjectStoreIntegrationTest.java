// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.ListQueryExpression;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStore.PersistResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStore.QuerySingleResult;
import io.vlingo.symbio.store.object.QueryExpression;

/**
 * JPAObjectStoreIntegrationTest
 *
 */
public class JPAObjectStoreIntegrationTest {
  private JPAObjectStoreDelegate delegate;
  private JPAObjectStore objectStore;
  private World world;

  @Test
  public void testThatObjectStoreInsertsOneAndQuerys() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    Person person = new Person(1L, 21, "Jody Jones");
    objectStore.persist(person, person.id, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    queryInterest.until = TestUntil.happenings(1);
    MapQueryExpression expression = MapQueryExpression.using(Person.class, null, MapQueryExpression.map("id", 1L));
    objectStore.queryObject(expression, queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(person, queryInterest.singleResult.get().persistentObject);

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(1);
    objectStore.remove(person, person.id, removeInterest);
    removeInterest.until.completes();
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQueries() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");
    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    QueryExpression queryExpression = QueryExpression.using(Person.class, "Person.allPersons");
    objectStore.queryAll(queryExpression, queryInterest);
    queryInterest.until.completes();
    final QueryMultiResults multiResults = queryInterest.multiResults.get();
    assertNotNull(multiResults);
    assertEquals(3, multiResults.persistentObjects.size());
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) multiResults.persistentObjects.iterator();
    assertEquals(person1, iterator.next());
    assertEquals(person2, iterator.next());
    assertEquals(person3, iterator.next());

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(3);
    objectStore.remove(person1, person1.id, removeInterest);
    objectStore.remove(person2, person2.id, removeInterest);
    objectStore.remove(person3, person3.id, removeInterest);
    removeInterest.until.completes();
  }

  @Test
  public void testThatSingleEntityUpdates() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    Person person1 = new Person(1L, 21, "Jody Jones");

    objectStore.persist(person1, person1.id, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestPersistResultInterest updateInterest = new TestPersistResultInterest();
    updateInterest.until = TestUntil.happenings(1);
    person1 = Person.newPersonFrom(person1);
    objectStore.persist(person1, person1.id, updateInterest);
    updateInterest.until.completes();
    assertEquals(Result.Success, updateInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    queryInterest.until = TestUntil.happenings(1);
    MapQueryExpression expression = MapQueryExpression.using(Person.class, null, MapQueryExpression.map("id", 1L));
    objectStore.queryObject(expression, queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    Person queriedPerson = (Person) queryInterest.singleResult.get().persistentObject;
    assertEquals(person1.id, queriedPerson.id);
    assertEquals(person1.age, queriedPerson.age);
    assertEquals(person1.name, queriedPerson.name);

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(1);
    objectStore.remove(person1, person1.id, removeInterest);
    removeInterest.until.completes();
  }

  @Test
  public void testThatMultipleEntitiesUpdate() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    QueryExpression queryExpression = QueryExpression.using(Person.class, "Person.allPersons");
    objectStore.queryAll(queryExpression, queryInterest);
    queryInterest.until.completes();

    Collection<?> resultCollection = queryInterest.multiResults.get().persistentObjects;
    assertNotNull(resultCollection);
    assertTrue(resultCollection.size() == 3);

    final QueryMultiResults mapResults = queryInterest.multiResults.get();
    final Iterator<Person> iterator = (Iterator<Person>) mapResults.persistentObjects.iterator();

    /*
     * TODO: persistentId not being set on queried entities
     */
    final List<Object> modifiedPersons = new ArrayList<>();
    while (iterator.hasNext()) {
      Person person = iterator.next();
      person = person.newPersonWithName(person.name + " " + person.id);
      // person.name + " " + person.id;
      modifiedPersons.add(person);
    }

    persistInterest.until = TestUntil.happenings(1);
    objectStore.persistAll(modifiedPersons, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(queryExpression, queryInterest);
    queryInterest.until.completes();

    Object[] queriedArray = queryInterest.multiResults.get().persistentObjects.toArray();
    assertArrayEquals(modifiedPersons.toArray(), queriedArray);
    for (int i = 0; i < 3; i++) {
      Person modifiedPerson = (Person) modifiedPersons.get(i);
      Person queriedPerson = (Person) queriedArray[i];
      assertEquals(modifiedPerson.id, queriedPerson.id);
      assertEquals(modifiedPerson.age, queriedPerson.age);
      assertEquals(modifiedPerson.name, queriedPerson.name);
    }

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(3);
    objectStore.remove(person1, person1.id, removeInterest);
    objectStore.remove(person2, person2.id, removeInterest);
    objectStore.remove(person3, person3.id, removeInterest);
    removeInterest.until.completes();
  }

  @Test
  public void testMultipleEntitiesParameterizedListQuery() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    ListQueryExpression queryExpression = ListQueryExpression.using(Person.class, "Person.adultsParmList",
            Arrays.asList(22));
    objectStore.queryAll(queryExpression, queryInterest);
    queryInterest.until.completes();
    final QueryMultiResults multiResults = queryInterest.multiResults.get();
    assertNotNull(multiResults);
    assertEquals(1, multiResults.persistentObjects.size());
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) multiResults.persistentObjects.iterator();
    assertEquals(person3, iterator.next());

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(3);
    objectStore.remove(person1, person1.id, removeInterest);
    objectStore.remove(person2, person2.id, removeInterest);
    objectStore.remove(person3, person3.id, removeInterest);
    removeInterest.until.completes();
  }

  @Test
  public void testMultipleEntitiesParameterizedMapQuery() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);

    Person person1 = new Person(1L, 21, "Jody Jones");
    Person person2 = new Person(2L, 21, "Joey Jones");
    Person person3 = new Person(3L, 25, "Mira Jones");

    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();
    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    Map<String, Integer> parmMap = Collections.singletonMap("age", 22);
    MapQueryExpression queryExpression = MapQueryExpression.using(Person.class, "Person.adultsParmMap", parmMap);
    objectStore.queryAll(queryExpression, queryInterest);
    queryInterest.until.completes();
    final QueryMultiResults multiResults = queryInterest.multiResults.get();
    assertNotNull(multiResults);
    assertEquals(1, multiResults.persistentObjects.size());
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) multiResults.persistentObjects.iterator();
    assertEquals(person3, iterator.next());

    // cleanup db
    final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
    removeInterest.until = TestUntil.happenings(3);
    objectStore.remove(person1, person1.id, removeInterest);
    objectStore.remove(person2, person2.id, removeInterest);
    objectStore.remove(person3, person3.id, removeInterest);
    removeInterest.until.completes();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    world = World.startWithDefaults("jpa-test");

    delegate = new JPAObjectStoreDelegate(world.stage());

    objectStore = world.actorFor(JPAObjectStore.class, JPAObjectStoreActor.class, delegate);

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
    public TestUntil until;

    @Override
    public void queryAllResultedIn(final Outcome<StorageException, Result> outcome, final QueryMultiResults results,
            final Object object) {
      multiResults.set(results);
      until.happened();
    }

    @Override
    public void queryObjectResultedIn(final Outcome<StorageException, Result> outcome, final QuerySingleResult result,
            final Object object) {
      singleResult.set(result);
      until.happened();
    }
  }

  private static class TestPersistResultInterest implements PersistResultInterest {
    public AtomicReference<Outcome<StorageException, Result>> outcome = new AtomicReference<>();
    public TestUntil until;

    @Override
    public void persistResultedIn(final Outcome<StorageException, Result> outcome, final Object persistentObject,
            final int possible, final int actual, final Object object) {
      this.outcome.set(outcome);
      until.happened();
    }
  }

}
