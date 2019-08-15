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
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEvents.PersonAdded;

public class JDBCObjectStoreEntryReaderTest extends JPAObjectStoreTest {
  protected EntryReader<Entry<String>> entryReader;

  @Test
  public void testThatEntryReaderReadsOne() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person(nextPersonId(), 21, "Jody Jones");
    final PersonAdded event = new PersonAdded(person);
    objectStore.persist(person, Arrays.asList(event), -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Entry<String> entry = entryReader.readNext().await();
    assertNotNull(entry);
  }

  @Test
  public void testThatEntryReaderReadsMany() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    final Person person = new Person(nextPersonId(), 21, "Jody Jones");
    final int totalEvents = 100;

    final List<Source<PersonAdded>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final PersonAdded event = new PersonAdded(person);
      events.add(event);
    }
    objectStore.persist(person, events, -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    for (long count = 1; count <= totalEvents; ) {
      final List<Entry<String>> entries = entryReader.readNext(5).await();
      assertNotNull(entries);
      assertEquals(5, entries.size());
      for (final Entry<String> entry : entries) {
        assertEquals(count++, Long.parseLong(entry.id()));
      }
    }
  }

  @Test
  public void testThatEntryReaderSeeksAround() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    Person person = new Person(nextPersonId(), 21, "Jody Jones");
    final int totalEvents = 20;
    final List<Source<PersonAdded>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final PersonAdded event = new PersonAdded(person);
      events.add(event);
    }
    objectStore.persist(person, events, -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    for (long count = 1; count <= totalEvents; ) {
      final Entry<String> entry = entryReader.readNext(String.valueOf(count)).await();
      assertEquals(count, Long.parseLong(entry.id()));
      count += 2;
    }
  }

  @Test
  public void testThatEntryReaderSeeksRandomly() {
    final TestPersistResultInterest persistInterest = new TestPersistResultInterest();
    final AccessSafely access = persistInterest.afterCompleting(1);
    Person person = new Person(nextPersonId(), 21, "Jody Jones");
    final int totalEvents = 25;
    final List<Source<PersonAdded>> events = new ArrayList<>(totalEvents);
    for (int idx = 1; idx <= totalEvents; ++idx) {
      final PersonAdded event = new PersonAdded(person);
      events.add(event);
    }
    objectStore.persist(person, events, -1L, persistInterest);
    final Outcome<StorageException, Result> outcome = access.readFrom("outcome");
    assertEquals(Result.Success, outcome.andThen(success -> success).get());

    final Random random = new Random();
    for (long count = 1; count <= totalEvents; ++count) {
      final int id = random.nextInt(totalEvents) + 1; // nextInt() returns 0 - 24
      final Entry<String> entry = entryReader.readNext(String.valueOf(id)).await();
      assertEquals(id, Long.parseLong(entry.id()));
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    entryReader = objectStore.entryReader("jdbi-entry-reader").await();
  }

  private static final AtomicLong nextId = new AtomicLong(0);
  private long nextPersonId() {
    final long id = nextId.incrementAndGet();
    return id;
  }
}
