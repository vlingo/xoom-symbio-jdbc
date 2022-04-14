// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.jdbc.jpa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.ListQueryExpression;
import io.vlingo.xoom.symbio.store.MapQueryExpression;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEvents.PersonAdded;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEvents.PersonRenamed;

/**
 * JPAObjectStoreIntegrationTest is responsible for testing {@link JPAObjectStore}
 */
public abstract class JPAObjectStoreIntegrationTest extends JPAObjectStoreTest {

    @Test
    public void testThatObjectStoreInsertsOneAndQuerys() {
        dispatcher.afterCompleting(1);

        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
        Person person = new Person(1L, 21, "Jody Jones");

        objectStore.persist(
          StateSources.of(person,new PersonAdded(person)),
            person.persistenceId(),
            persistInterest);
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

        final Map<String, Dispatchable<Entry<String>, State<String>>> dispatched = dispatcher.getDispatched();
        assertEquals(1, dispatched.size());

        for (final Dispatchable<Entry<String>, State<String>> dispatchable : dispatched.values()) {
            Assert.assertNotNull(dispatchable.createdOn());
            Assert.assertNotNull(dispatchable.id());
            final List<Entry<String>> dispatchedEntries = dispatchable.entries();
            Assert.assertEquals(1, dispatchedEntries.size());
            for (final Entry<String> dispatchedEntry : dispatchedEntries) {
                Assert.assertTrue(dispatchedEntry.id() != null && !dispatchedEntry.id().isEmpty());
            }
        }
    }

    @Test
    public void testThatObjectStoreInsertsMultipleAndQueries() {
        dispatcher.afterCompleting(3);

        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
        Person person1 = new Person(1L, 21, "Jody Jones");
        Person person2 = new Person(2L, 21, "Joey Jones");
        Person person3 = new Person(3L, 25, "Mira Jones");
        objectStore.persistAll(
            Arrays.asList(
              StateSources.of(person1, new PersonAdded(person1)),
              StateSources.of(person2, new PersonAdded(person2)),
              StateSources.of(person3, new PersonAdded(person3))
            ),
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

        final Map<String, Dispatchable<Entry<String>, State<String>>> dispatched = dispatcher.getDispatched();
        assertEquals(3, dispatched.size());
    }

    @Test
    public void testThatSingleEntityUpdates() {
        dispatcher.afterCompleting(2);

        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);
        Person person1 = new Person(1L, 21, "Jody Jones");

        objectStore.persist(StateSources.of(person1, new PersonAdded(person1)), person1.persistenceId(), persistInterest);
        assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

        final TestPersistResultInterest updateInterest = new TestPersistResultInterest();
        final AccessSafely updateInterestAccess = updateInterest.afterCompleting(1);
        person1 = person1.newPersonWithName( person1.name + " " + person1.persistenceId() );
        objectStore.persist(StateSources.of(person1, new PersonRenamed(person1)), person1.persistenceId(), updateInterest);
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

        final Map<String, Dispatchable<Entry<String>, State<String>>> dispatched = dispatcher.getDispatched();
        assertEquals(2, dispatched.size());

        // cleanup db
        final TestPersistResultInterest removeInterest = new TestPersistResultInterest();
        final AccessSafely removeInterestAccess = removeInterest.afterCompleting(1);
        objectStore.remove(person1, person1.persistenceId(), removeInterest);
        assertEquals(Result.Success, removeInterestAccess.readFrom("result"));
    }

    @Test
    public void testThatMultipleEntitiesUpdate() {
        dispatcher.afterCompleting(6);
        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);

        Person person1 = new Person(1L, 21, "Jody Jones");
        Person person2 = new Person(2L, 21, "Joey Jones");
        Person person3 = new Person(3L, 25, "Mira Jones");

        objectStore.persistAll(
            Arrays.asList(
              StateSources.of(person1, new PersonAdded(person1)),
              StateSources.of(person2, new PersonAdded(person2)),
              StateSources.of(person3, new PersonAdded(person3))
            ),
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
            modifiedPersons.stream().map(p -> StateSources.of(p, new PersonRenamed(p))).collect(Collectors.toList()),
            updateInterest);
        assertEquals(Result.Success, updateInterestAccess.readFrom("result"));

        final TestQueryResultInterest queryInterest2 = new TestQueryResultInterest();
        final AccessSafely queryInterest2Access = queryInterest2.afterCompleting(1);
        queryInterest.multiResults.set(null);
        objectStore.queryAll(queryExpression, queryInterest2);

        assertEquals(3, (int)queryInterest2Access.readFrom("multiResultsSize"));
        for (int i = 0; i < 3; i++) {
            Person modifiedPerson = modifiedPersons.get(i);
            Person queriedPerson = (Person) queryInterest2Access.readFrom("multiResultsValue", i);
            assertEquals(modifiedPerson.persistenceId(), queriedPerson.persistenceId());
            assertEquals(modifiedPerson.age, queriedPerson.age);
            assertEquals(modifiedPerson.name, queriedPerson.name);
        }

        final Map<String, Dispatchable<Entry<String>, State<String>>> dispatched = dispatcher.getDispatched();
        assertEquals(6, dispatched.size());

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
            Arrays.asList(
              StateSources.of(person1, new PersonAdded(person1)),
              StateSources.of(person2, new PersonAdded(person2)),
              StateSources.of(person3, new PersonAdded(person3))
            ),
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
            Arrays.asList(
              StateSources.of(person1, new PersonAdded(person1)),
              StateSources.of(person2, new PersonAdded(person2)),
              StateSources.of(person3, new PersonAdded(person3))
            ),
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


    @Test
    public void testRedispatch() {
        final AccessSafely accessDispatcher = dispatcher.afterCompleting(5);

        accessDispatcher.writeUsing("processDispatch", false);

        final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
        final AccessSafely persistInterestAccess = persistInterest.afterCompleting(1);

        final Person person1 = new Person(1L, 21, "Jody Jones");
        final Person person2 = new Person(2L, 21, "Joey Jones");
        final Person person3 = new Person(3L, 25, "Mira Jones");

        objectStore.persistAll(
            Arrays.asList(
              StateSources.of(person1, new PersonAdded(person1)),
              StateSources.of(person2, new PersonAdded(person2)),
              StateSources.of(person3, new PersonAdded(person3))
            ),
            persistInterest);
        assertEquals(Result.Success, persistInterestAccess.readFrom("result"));

        try {
            Thread.sleep(3000);
        } catch (InterruptedException ex) {
            //ignored
        }

        accessDispatcher.writeUsing("processDispatch", true);

        final Map<String, Dispatchable<Entry<String>, State<String>>> dispatched = dispatcher.getDispatched();
        assertEquals(3, dispatched.size());

        final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
        assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

        for (final Dispatchable<Entry<String>, State<String>> dispatchable : dispatched.values()) {
            Assert.assertNotNull(dispatchable.createdOn());
            Assert.assertNotNull(dispatchable.id());
            final List<Entry<String>> dispatchedEntries = dispatchable.entries();
            Assert.assertEquals(3, dispatchedEntries.size());
            for (final Entry<String> dispatchedEntry : dispatchedEntries) {
                Assert.assertTrue(dispatchedEntry.id() != null && !dispatchedEntry.id().isEmpty());
                Assert.assertEquals(PersonAdded.class, dispatchedEntry.typed());
            }
        }
    }
}
