// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.reactivestreams.sink.ConsumerSink;
import io.vlingo.xoom.symbio.*;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.MockDispatcher;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.StateObjectMapper;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.JPAObjectStoreTest;
import org.jdbi.v3.core.statement.SqlStatement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.*;

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
    person.incrementVersion();
    final long entryVersion = person.version();
    final Event event = new Event("test-event");
    objectStore.persist(StateSources.of(person, event), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Entry<String> entry = entryReader.readNext().await();
    assertNotNull(entry);
    assertEquals(entryVersion, entry.entryVersion());

    // Check gap prevention for one entry
    final Entry<String> empty = entryReader.readNext().await();
    assertNull(empty);
  }

  @Test
  public void testThatEntryReaderReadsMany() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    person.incrementVersion();
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
        assertTrue(entry.entryVersion() > 0);
      }
    }

    // check gap prevention for multiple entries
    final int fewEvents = 7;
    final List<Source<Event>> events2 = new ArrayList<>(fewEvents);
    for (int idx = 1; idx <= fewEvents; ++idx) {
      final Event event = new Event("test-event-" + idx);
      events2.add(event);
    }
    final JPAObjectStoreTest.TestPersistResultInterest persistInterest2 = new JPAObjectStoreTest.TestPersistResultInterest();
    final AccessSafely access2 = persistInterest2.afterCompleting(1);
    objectStore.persist(StateSources.of(person, events2), -1L, persistInterest2);
    final Outcome<StorageException, Result> outcome2 = access2.readFrom("outcome");
    assertEquals(Result.Success, outcome2.andThen(success -> success).get());

    // read more events than available
    final List<Entry<String>> entries = entryReader.readNext(fewEvents + 4).await();
    assertEquals(fewEvents, entries.size());
  }

  @Test
  public void testThatEntryReaderSeeksAround() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    person.incrementVersion();
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
      assertTrue(entry.entryVersion() > 0);
      count += 10;
    }
  }

  @Test
  public void testThatEntryReaderSeeksRandomly() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person("Jody Jones", 21, 1L);
    person.incrementVersion();
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
      assertTrue(entry.entryVersion() > 0);
    }
  }

  private ConsumerSink<EntryBundle> sink;

  private AtomicInteger totalSources = new AtomicInteger(0);

  @Test
  public void testThatJournalReaderStreams() {
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

    access.writingWith("sourcesCounter", (state) -> { totalSources.incrementAndGet(); });
    access.readingWith("sourcesCount", () -> totalSources.get());

    final Stream all = entryReader.streamAll().await();

    final Consumer<EntryBundle> bundles = (bundle) -> access.writeUsing("sourcesCounter", 1);

    sink = new ConsumerSink<>(bundles);

    all.flowInto(sink, 20);

    final int sourcesCount = access.readFromExpecting("sourcesCount", totalEvents);

    Assert.assertEquals(totalEvents, totalSources.get());
    Assert.assertEquals(totalSources.get(), sourcesCount);
  }

  @Before
  public void setUp() throws Exception {
    jdbi = jdbiOnDatabase();

    try (final Connection initConnection = configuration().connectionProvider.connection()) {
      jdbi.createCommonTables(initConnection);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create common tables because: " + e.getMessage(), e);
    }

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
    entryReader.close();
    jdbi.close();
    world.terminate();
  }

  protected abstract JdbiOnDatabase jdbiOnDatabase() throws Exception;

  protected abstract Configuration configuration();
}
