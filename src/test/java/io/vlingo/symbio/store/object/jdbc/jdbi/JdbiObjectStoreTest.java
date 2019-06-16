// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;
import io.vlingo.symbio.store.object.ListQueryExpression;
import io.vlingo.symbio.store.object.MapQueryExpression;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMode;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreActor;
import io.vlingo.symbio.store.object.jdbc.Person;

public class JdbiObjectStoreTest {
  private Handle handle;
  // private Jdbi jdbi;
  private JdbiObjectStoreDelegate delegate;
  private ObjectStore objectStore;
  private World world;

  @Test
  public void testThatObjectStoreConnects() {
    assertNotNull(objectStore);
  }

  @Test
  public void testThatObjectStoreInsertsOneAndQuerys() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    objectStore.persist(person, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    // Map
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            MapQueryExpression.using(
                    Person.class,
                    "SELECT * FROM PERSON WHERE id = :id",
                    MapQueryExpression.map("id", 1L)),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(person, queryInterest.singleResult.get().persistentObject);

    // List
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            ListQueryExpression.using(
                    Person.class,
                    "SELECT * FROM PERSON WHERE id = <listArgValues>",
                    Arrays.asList(1L)),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(person, queryInterest.singleResult.get().persistentObject);
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQuerys() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    final Person person1 = new Person("Jody Jones", 21, 1L);
    final Person person2 = new Person("Joey Jones", 21, 2L);
    final Person person3 = new Person("Mira Jones", 25, 3L);
    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(
            QueryExpression.using(Person.class, "SELECT * FROM PERSON"),
            queryInterest);
    queryInterest.until.completes();
    final QueryMultiResults mapResults = queryInterest.multiResults.get();
    assertNotNull(mapResults);
    assertEquals(3, mapResults.persistentObjects.size());
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) mapResults.persistentObjects.iterator();
    assertEquals(person1, iterator.next());
    assertEquals(person2, iterator.next());
    assertEquals(person3, iterator.next());
  }

  @Test
  public void testThatSingleEntityUpdates() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    objectStore.persist(person, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    // Map
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            MapQueryExpression.using(
                    Person.class,
                    "SELECT * FROM PERSON WHERE id = :id",
                    QueryMode.ReadUpdate,
                    MapQueryExpression.map("id", 1L)),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(person, queryInterest.singleResult.get().persistentObject);

    persistInterest.until = TestUntil.happenings(1);
    final Person queriedPerson = queryInterest.singleResult.get().persistentObject();
    final Person modifiedPerson = queriedPerson.withName("Jody Mojo Jojo");
    objectStore.persist(modifiedPerson, queryInterest.singleResult.get().updateId, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    // List
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            ListQueryExpression.using(
                    Person.class,
                    "SELECT * FROM PERSON WHERE id = <listArgValues>",
                    Arrays.asList(1L)),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(modifiedPerson, queryInterest.singleResult.get().persistentObject);
  }

  @Test
  public void testThatMultipleEntitiesUpdate() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    persistInterest.until = TestUntil.happenings(1);
    final Person person1 = new Person("Jody Jones", 21, 1L);
    final Person person2 = new Person("Joey Jones", 21, 2L);
    final Person person3 = new Person("Mira Jones", 25, 3L);
    objectStore.persistAll(Arrays.asList(person1, person2, person3), persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(
            QueryExpression.using(Person.class, "SELECT * FROM PERSON ORDER BY id", QueryMode.ReadUpdate),
            queryInterest);
    queryInterest.until.completes();

    assertTrue(queryInterest.multiResults.get().updateId > 0);

    final QueryMultiResults mapResults = queryInterest.multiResults.get();
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) mapResults.persistentObjects.iterator();
    final List<Person> modifiedPersons = new ArrayList<>();
    while (iterator.hasNext()) {
      final Person person = iterator.next();
      modifiedPersons.add(person.withName(person.name + " " + person.id));
    }
    persistInterest.until = TestUntil.happenings(1);
    objectStore.persistAll(modifiedPersons, queryInterest.multiResults.get().updateId, persistInterest);
    persistInterest.until.completes();
    assertEquals(Result.Success, persistInterest.outcome.get().andThen(success -> success).get());

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(
            QueryExpression.using(Person.class, "SELECT * FROM PERSON ORDER BY id"),
            queryInterest);
    queryInterest.until.completes();

    assertArrayEquals(modifiedPersons.toArray(), queryInterest.multiResults.get().persistentObjects.toArray());
  }

  @Before
  public void setUp() throws Exception {
    final Configuration configuration = HSQLDBConfigurationProvider.testConfiguration(DataFormat.Native);

    handle = Jdbi.open(configuration.connection);

    handle.execute("CREATE TABLE PERSON (id BIGINT PRIMARY KEY, name VARCHAR(200), age INTEGER)");
    handle.execute("CREATE TABLE EVENT_JOURNAL (E_ID BIGINT PRIMARY KEY, E_TYPE VARCHAR(200), E_TYPE_VERSION INTEGER, E_DATA LONGVARCHAR(65535), E_METADATA_VALUE VARCHAR(4000) NULL, E_METADATA_OP VARCHAR(128) NULL)");

    final PersistentObjectMapper personMapper =
            PersistentObjectMapper.with(
                    Person.class,
                    JdbiPersistMapper.with(
                            "INSERT INTO PERSON(id, name, age) VALUES (:id, :name, :age)",
                            "UPDATE PERSON SET name = :name, age = :age WHERE id = :id",
                            (update,object) -> update.bindFields(object)),
                    new PersonMapper());

    final PersistentObjectMapper entryMapper =
            PersistentObjectMapper.with(
                    Entry.class,
                    JdbiPersistMapper.with(
                            "INSERT INTO EVENT_JOURNAL(E_ID, E_TYPE, E_TYPE_VERSION, E_DATA, E_METADATA_VALUE, E_METADATA_OP) "
                             + "VALUES (:id, :type, :typeVersion, :entryData, :metadata.value, :metadata.op)",
                            "(unused)",
                            (update,object) -> update.bindFields(object)),
                    null); // query mapper not used

    world = World.startWithDefaults("jdbi-test");

    delegate = new JdbiObjectStoreDelegate(world.stage(), configuration);

    objectStore = world.actorFor(ObjectStore.class, JDBCObjectStoreActor.class, delegate);

    objectStore.registerMapper(personMapper);
    objectStore.registerMapper(entryMapper);
  }

  @After
  public void tearDown() {
    objectStore.close();
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
    public void persistResultedIn(Outcome<StorageException, Result> outcome, Object persistentObject, int possible, int actual, Object object) {
      this.outcome.set(outcome);
      until.happened();
    }
  }
}
