// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.jdbi.v3.core.statement.SqlStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.MockDispatcher;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.StateObjectMapper;
import io.vlingo.symbio.store.object.StateSources;

public abstract class JdbiObjectStoreEntryReaderTest {
  protected MockDispatcher<BaseEntry.TextEntry, State.TextState> dispatcher;
  protected EntryReader<Entry<String>> entryReader;
  protected JdbiOnDatabase jdbi;
  protected ObjectStore objectStore;
  protected World world;

  @Test
  public void testThatEntryReaderReadsOne() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    final Event event = new Event("test-event");
    objectStore.persist(StateSources.of(person, event), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Entry<String> entry = entryReader.readNext().await();
    assertNotNull(entry);
  }

  @Test
  public void testThatEntryReaderReadsMany() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    final int totalEvents = 100;
    final List<Source<Event>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final Event event = new Event("test-event-" + idx);
      events.add(event);
    }
    objectStore.persist(StateSources.of(person, events), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    for (long count = 1; count <= totalEvents; ) {
      final List<Entry<String>> entries = entryReader.readNext(10).await();
      assertEquals(10, entries.size());
      for (final Entry<String> entry : entries) {
        assertEquals(count++, Long.parseLong(entry.id()));
      }
    }
  }

  @Test
  public void testThatEntryReaderSeeksAround() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    final int totalEvents = 100;
    final List<Source<Event>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final Event event = new Event("test-event-" + idx);
      events.add(event);
    }
    objectStore.persist(StateSources.of(person, events), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    for (long count = 1; count <= totalEvents; ) {
      final Entry<String> entry = entryReader.readNext(String.valueOf(count)).await();
      assertEquals(count, Long.parseLong(entry.id()));
      count += 10;
    }
  }

  @Test
  public void testThatEntryReaderSeeksRandomly() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    final int totalEvents = 100;
    final List<Source<Event>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final Event event = new Event("test-event-" + idx);
      events.add(event);
    }
    objectStore.persist(StateSources.of(person, events), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Random random = new Random();
    for (long count = 1; count <= totalEvents; ++count) {
      final int id = random.nextInt(100) + 1; // nextInt() returns 0 - 99
      final Entry<String> entry = entryReader.readNext(String.valueOf(id)).await();
      assertEquals(id, Long.parseLong(entry.id()));
    }
  }

  @Before
  public void setUp() throws Exception {
    jdbi = jdbiOnDatabase();
    jdbi.createCommonTables();
    jdbi.handle().execute("CREATE TABLE PERSON (id BIGINT PRIMARY KEY, name VARCHAR(200), age INTEGER)");

    world = World.startWithDefaults("entry-reader-test");

    dispatcher = new MockDispatcher<>();

    final StateObjectMapper personMapper =
            StateObjectMapper.with(
                    Person.class,
                    JdbiPersistMapper.with(
                            "INSERT INTO PERSON(id, name, age) VALUES (:id, :name, :age)",
                            "UPDATE PERSON SET name = :name, age = :age WHERE id = :id",
                            SqlStatement::bindFields),
                    new PersonMapper());

    objectStore = jdbi.objectStore(world, Arrays.asList(dispatcher), Collections.singletonList(personMapper));

    entryReader = objectStore.entryReader("jdbi-entry-reader").await();
  }

  @After
  public void tearDown() {
    objectStore.close();
    world.terminate();
  }

  protected abstract JdbiOnDatabase jdbiOnDatabase() throws Exception;
}
