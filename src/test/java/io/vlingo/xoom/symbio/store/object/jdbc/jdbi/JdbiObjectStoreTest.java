// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.*;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.actors.testkit.TestUntil;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.ListQueryExpression;
import io.vlingo.xoom.symbio.store.MapQueryExpression;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.QueryMode;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.MockDispatcher;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryResultInterest;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.xoom.symbio.store.object.PersistentEntry;
import io.vlingo.xoom.symbio.store.object.StateObjectMapper;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;

public class JdbiObjectStoreTest {
  private JdbiOnDatabase jdbi;
  private ObjectStore objectStore;
  private World world;
  private MockDispatcher<BaseEntry.TextEntry, State.TextState> dispatcher;

  @Test
  public void testThatObjectStoreConnects() {
    assertNotNull(objectStore);
  }

  @Test
  public void testThatObjectStoreInsertsOneAndQuerys() {
    dispatcher.afterCompleting(1);

    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    objectStore.persist(StateSources.of(person), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

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
    assertEquals(person, queryInterest.singleResult.get().stateObject);

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
    assertEquals(person, queryInterest.singleResult.get().stateObject);

    final Map<String, Dispatchable<BaseEntry.TextEntry, State.TextState>> dispatched = dispatcher.getDispatched();
    assertEquals(1, dispatched.size());
  }

  @Test
  public void testThatObjectStoreInsertsOneWithEventAndQuerys() {
    dispatcher.afterCompleting(1);

    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    final Event event = new Event("test-event");
    objectStore.persist(StateSources.of(person, event), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Map<String, Dispatchable<BaseEntry.TextEntry, State.TextState>> dispatched = dispatcher.getDispatched();
    assertEquals(1, dispatched.size());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    // Person
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            ListQueryExpression.using(
                    Person.class,
                    "SELECT * FROM PERSON WHERE id = <listArgValues>",
                    Arrays.asList(1L)),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());
    assertEquals(person, queryInterest.singleResult.get().stateObject);

    // Event
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryObject(
            ListQueryExpression.using(Entry.class, "SELECT * FROM " + JDBCObjectStoreEntryJournalQueries.EntryJournalTableName),
            queryInterest);
    queryInterest.until.completes();
    assertNotNull(queryInterest.singleResult.get());

    final EntryAdapterProvider provider = EntryAdapterProvider.instance(world);
    final PersistentEntry entry = queryInterest.singleResult.get().stateObject();
    assertEquals(event, entry.asSource(provider));
  }

  @Test
  public void testThatObjectStoreInsertsMultipleAndQuerys() {
    dispatcher.afterCompleting(3);

    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person1 = new Person("Jody Jones", 21, 1L);
    final Person person2 = new Person("Joey Jones", 21, 2L);
    final Person person3 = new Person("Mira Jones", 25, 3L);
    objectStore.persistAll(Arrays.asList(StateSources.of(person1), StateSources.of(person2), StateSources.of(person3)), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Map<String, Dispatchable<BaseEntry.TextEntry, State.TextState>> dispatched = dispatcher.getDispatched();
    assertEquals(3, dispatched.size());

    final TestQueryResultInterest queryInterest = new TestQueryResultInterest();

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(
            QueryExpression.using(Person.class, "SELECT * FROM PERSON"),
            queryInterest);
    queryInterest.until.completes();
    final QueryMultiResults mapResults = queryInterest.multiResults.get();
    assertNotNull(mapResults);
    assertEquals(3, mapResults.stateObjects.size());
    @SuppressWarnings("unchecked")
    final Iterator<Person> iterator = (Iterator<Person>) mapResults.stateObjects.iterator();
    assertEquals(person1, iterator.next());
    assertEquals(person2, iterator.next());
    assertEquals(person3, iterator.next());
  }

  @Test
  public void testThatSingleEntityUpdates() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    objectStore.persist(StateSources.of(person), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

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
    assertEquals(person, queryInterest.singleResult.get().stateObject);

    final AccessSafely access1 = persistInterest.afterCompleting(1);
    final Person queriedPerson = queryInterest.singleResult.get().stateObject();
    final Person modifiedPerson = queriedPerson.withName("Jody Mojo Jojo");
    objectStore.persist(StateSources.of(modifiedPerson), queryInterest.singleResult.get().updateId, persistInterest);
    final Outcome<StorageException, Result> outcome1 = access1.readFrom("outcome");
    assertEquals(Result.Success, outcome1.andThen(success -> success).get());

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
    assertEquals(modifiedPerson, queryInterest.singleResult.get().stateObject);
  }

  @Test
  public void testThatMultipleEntitiesUpdate() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person1 = new Person("Jody Jones", 21, 1L);
    final Person person2 = new Person("Joey Jones", 21, 2L);
    final Person person3 = new Person("Mira Jones", 25, 3L);

    objectStore.persistAll(Arrays.asList(StateSources.of(person1), StateSources.of(person2), StateSources.of(person3)), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

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
    final Iterator<Person> iterator = (Iterator<Person>) mapResults.stateObjects.iterator();
    final List<Person> modifiedPersons = new ArrayList<>();
    while (iterator.hasNext()) {
      final Person person = iterator.next();
      modifiedPersons.add(person.withName(person.name + " " + person.id));
    }
    final AccessSafely access1 = persistInterest.afterCompleting(1);
    objectStore.persistAll(modifiedPersons.stream().map(StateSources::of).collect(Collectors.toList()), queryInterest.multiResults.get().updateId, persistInterest);
    final Outcome<StorageException, Result> outcome1 = access1.readFrom("outcome");
    assertEquals(Result.Success, outcome1.andThen(success -> success).get());

    queryInterest.multiResults.set(null);
    queryInterest.until = TestUntil.happenings(1);
    objectStore.queryAll(
            QueryExpression.using(Person.class, "SELECT * FROM PERSON ORDER BY id"),
            queryInterest);
    queryInterest.until.completes();

    assertArrayEquals(modifiedPersons.toArray(), queryInterest.multiResults.get().stateObjects.toArray());
  }

  @Test
  public void testRedispatch() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(5);

    accessDispatcher.writeUsing("processDispatch", false);

    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person1 = new Person("Jody Jones", 21, 1L);
    final Person person2 = new Person("Joey Jones", 21, 2L);
    final Person person3 = new Person("Mira Jones", 25, 3L);

    final Event event1 = new Event("test-event");
    final Event event2 = new Event("test-event2");
    final Event event3 = new Event("test-event3");

    objectStore.persistAll(Arrays.asList(StateSources.of(person1, event1), StateSources.of(person2, event2), StateSources.of(person3, event3)), persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    accessDispatcher.writeUsing("processDispatch", true);

    final Map<String, Dispatchable<BaseEntry.TextEntry, State.TextState>> dispatched = dispatcher.getDispatched();
    assertEquals(3, dispatched.size());

    final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

    for (final Dispatchable<BaseEntry.TextEntry, State.TextState> dispatchable : dispatched.values()) {
      Assert.assertNotNull(dispatchable.createdOn());
      Assert.assertNotNull(dispatchable.id());
      final Collection<BaseEntry.TextEntry> dispatchedEntries = dispatchable.entries();
      Assert.assertEquals(3, dispatched.size());
      for (final BaseEntry.TextEntry dispatchedEntry : dispatchedEntries) {
        Assert.assertTrue(dispatchedEntry.id() != null && !dispatchedEntry.id().isEmpty());
        Assert.assertEquals(event1.getClass(), dispatchedEntry.typed());
      }
    }
  }

  @Test
  public void testThatDatabaseTypeIsHSQLDB() {
    assertEquals(DatabaseType.HSQLDB, jdbi.databaseType());
  }

  @Before
  public void setUp() throws Exception {
    final Configuration configuration = HSQLDBConfigurationProvider.testConfiguration(DataFormat.Native);

    jdbi = JdbiOnHSQLDB.openUsing(configuration);

    jdbi.handle().execute("DROP SCHEMA PUBLIC CASCADE");
    jdbi.handle().execute("CREATE TABLE PERSON (id BIGINT PRIMARY KEY, name VARCHAR(200), age INTEGER)");

    try (final Connection initConnection = configuration.connectionProvider.newConnection()) {
      try {
        initConnection.setAutoCommit(true);
        jdbi.createTextEntryJournalTable(initConnection);
        jdbi.createDispatchableTable(initConnection);
        initConnection.setAutoCommit(false);
      } catch (Exception e) {
        initConnection.setAutoCommit(false);
        throw new RuntimeException("Failed to create common tables because: " + e.getMessage(), e);
      }
    }

    world = World.startWithDefaults("jdbi-test");

    dispatcher = new MockDispatcher<>();

    final StateObjectMapper personMapper =
            StateObjectMapper.with(
                    Person.class,
                    JdbiPersistMapper.with(
                            "INSERT INTO PERSON(id, name, age) VALUES (:id, :name, :age)",
                            "UPDATE PERSON SET name = :name, age = :age WHERE id = :id",
                            (update,object) -> update.bindFields(object)),
                    new PersonMapper());

    objectStore = jdbi.objectStore(world, Arrays.asList(dispatcher), Collections.singletonList(personMapper));
  }

  @After
  public void tearDown() {
    objectStore.close();
    jdbi.close();
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
}
