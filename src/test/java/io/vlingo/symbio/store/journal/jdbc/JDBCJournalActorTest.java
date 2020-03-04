// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Completes;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.reactivestreams.Stream;
import io.vlingo.reactivestreams.sink.ConsumerSink;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.EntryBundle;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.common.MockDispatcher;
import io.vlingo.symbio.store.common.event.TestEvent;
import io.vlingo.symbio.store.common.event.TestEventAdapter;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.journal.EntityStream;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;

public abstract class JDBCJournalActorTest extends BasePostgresJournalTest {
    private Entity1Adapter entity1Adapter = new Entity1Adapter();
    private Object object = new Object();
    private MockAppendResultInterest interest;
    private Journal<String> journal;
    private MockDispatcher<Entry<String>, TextState> dispatcher;
    private JournalReader<TextEntry> journalReader;
    private StreamReader<String> streamReader;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        interest = new MockAppendResultInterest();

        dispatcher = new MockDispatcher<>();

        journal =  world.stage().actorFor(Journal.class, JDBCJournalActor.class, dispatcher, configuration);
        EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());
        StateAdapterProvider.instance(world).registerAdapter(Entity1.class, entity1Adapter);

        final Completes<JournalReader<TextEntry>> completesJournalReader = journal.journalReader(streamName);
        journalReader = completesJournalReader.await();
        streamReader = journal.streamReader(streamName).await();
    }

    @Test
    public void testThatInsertsANewEvent() {
        dispatcher.afterCompleting(1);

        TestEvent appendedEvent = newEventForData(1);
        journal.append(streamName, 1, appendedEvent, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();

        assertEquals(1, dispatched.size());

        Entry<String> entry = journalReader.readNext().await();
        assertNotNull(entry);
        assertTrue(entry.entryVersion() > 0);
        TestEvent event = gson.fromJson(entry.entryData(), TestEvent.class);
        assertEquals(appendedEvent, event);
    }

    @Test
    public void testThatInsertsANewListOfEvents() {
        dispatcher.afterCompleting(1);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        journal.appendAll(streamName, 1, asList(appendedEvent1, appendedEvent2), interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(1, dispatched.size());

        List<TextEntry> eventStream = journalReader.readNext(2).await();
        Entry<String> entry1 = eventStream.get(0);
        assertNotNull(entry1);
        assertTrue(entry1.entryVersion() > 0);
        TestEvent event1 = gson.fromJson(entry1.entryData(), TestEvent.class);
        assertEquals(appendedEvent1, event1);

        Entry<String> entry2 = eventStream.get(1);
        assertNotNull(entry2);
        assertTrue(entry2.entryVersion() > 0);
        TestEvent event2 = gson.fromJson(entry2.entryData(), TestEvent.class);
        assertEquals(appendedEvent2, event2);
    }

    @Test
    public void testThatInsertsANewEventWithASnapshot() {
        dispatcher.afterCompleting(3);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        Entity1 entity = new Entity1("1", 123);

        journal.appendWith(streamName, 1, appendedEvent1, null, interest, object);
        journal.appendWith(streamName, 2, appendedEvent2, null, interest, object);
        journal.appendWith(streamName, 3, appendedEvent3, entity, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(3, dispatched.size());

        EntityStream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithASnapshot() {
        dispatcher.afterCompleting(2);
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        TestEvent appendedEvent4 = newEventForData(4);
        Entity1 entity = new Entity1("1", 123);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, appendedEvent4), entity, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(2, dispatched.size());

        EntityStream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithErrorWithoutASnapshot() {
        dispatcher.afterCompleting(0);
        System.out.println("========== BEGIN: testThatInsertsANewListOfEventsWithErrorWithoutASnapshot()");
        System.out.println("========== BEGIN: EXPECTED EXCEPTIONS AHEAD");
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent3 = newEventForData(3);

        final AccessSafely access = interest.afterCompleting(2);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, null), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, null), null, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(0, dispatched.size());

        assertEquals(0, (int) access.readFrom("successCount"));
        assertEquals(2, (int) access.readFrom("failureCount"));
    }

    @Test
    public void testThatReturnsSameReaderForSameName() {
        final String name = UUID.randomUUID().toString();

        final JournalReader<Entry<String>> journalReader1 = journal.journalReader(name).await();
        final JournalReader<Entry<String>> journalReader2 = journal.journalReader(name).await();
        assertEquals(journalReader1, journalReader2);

        final StreamReader<String> streamReader1 = journal.streamReader(name).await();
        final StreamReader<String> streamReader2 = journal.streamReader(name).await();
        assertEquals(streamReader1, streamReader2);
    }

    @Test
    public void testRedispatch() {
        interest.afterCompleting(2);
        final AccessSafely accessDispatcher = dispatcher.afterCompleting(4);

        accessDispatcher.writeUsing("processDispatch", false);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        TestEvent appendedEvent4 = newEventForData(4);
        Entity1 entity = new Entity1("1", 123);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, appendedEvent4), entity, interest, object);


        try {
          Thread.sleep(3000);
        } catch (InterruptedException ex) {
          //ignored
        }

        accessDispatcher.writeUsing("processDispatch", true);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(2, dispatched.size());

        final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
        assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

        for (final Dispatchable<Entry<String>, TextState> dispatchable : dispatched.values()) {
          Assert.assertNotNull(dispatchable.createdOn());
          Assert.assertNotNull(dispatchable.id());
          final Collection<Entry<String>> dispatchedEntries = dispatchable.entries();
          Assert.assertEquals(2, dispatchedEntries.size());

          for (final Entry<String> dispatchedEntry : dispatchedEntries) {
            Assert.assertTrue(dispatchedEntry.id() != null && !dispatchedEntry.id().isEmpty());
          }
        }
    }

    private ConsumerSink<EntryBundle> sink;

    private AtomicInteger totalSources = new AtomicInteger(0);

    @Test
    public void testThatJournalReaderStreams() {
      final int limit = 200;

      for (int count = 0; count < limit; ++count) {
        journal.append("123-" + count, 1, new TestEvent("123-" + count, count), interest, object);
      }

      final AccessSafely access = AccessSafely.afterCompleting(limit);

      access.writingWith("sourcesCounter", (state) -> { totalSources.incrementAndGet(); });
      access.readingWith("sourcesCount", () -> totalSources.get());

      final Stream all = journal.journalReader("test").andThenTo(reader -> reader.streamAll()).await();

      final Consumer<EntryBundle> bundles = (bundle) -> access.writeUsing("sourcesCounter", 1);

      sink = new ConsumerSink<>(bundles);

      all.flowInto(sink, 50);

      final int sourcesCount = access.readFromExpecting("sourcesCount", limit);

      Assert.assertEquals(limit, totalSources.get());
      Assert.assertEquals(totalSources.get(), sourcesCount);
    }

    private TestEvent newEventForData(int number) {
          final TestEvent event = new TestEvent(String.valueOf(number), number);
          return event;
      }

      public static final class Entity1 {
        public final String id;
        public final int number;

        public Entity1(final String id, final int number) {
          this.id = id;
          this.number = number;
        }
      }

      public static final class Entity1Adapter implements StateAdapter<Entity1,TextState> {

        @Override
        public int typeVersion() {
          return 1;
        }

        @Override
        public Entity1 fromRawState(TextState raw) {
          return JsonSerialization.deserialized(raw.data, raw.typed());
        }

        @Override
        public <ST> ST fromRawState(final TextState raw, final Class<ST> stateType) {
          return JsonSerialization.deserialized(raw.data, stateType);
        }

        @Override
        public TextState toRawState(Entity1 state, int stateVersion, Metadata metadata) {
          final String serialization = JsonSerialization.serialized(state);
          return new TextState(state.id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
        }

        @Override
        public TextState toRawState(final String id, final Entity1 state, final int stateVersion, final Metadata metadata) {
          final String serialization = JsonSerialization.serialized(state);
          return new TextState(id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
        }
      }
  }