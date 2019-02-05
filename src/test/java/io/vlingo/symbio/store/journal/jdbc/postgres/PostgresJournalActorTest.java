// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;

public class PostgresJournalActorTest extends BasePostgresJournalTest {
    private Entity1Adapter entity1Adapter = new Entity1Adapter();
    private Object object = new Object();
    private MockAppendResultInterest interest;
    private Journal<String> journal;
    private JournalListener<String> listener;
    private JournalReader<String> journalReader;
    private StreamReader<String> streamReader;
    private TestUntil until;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        interest = new MockAppendResultInterest();
        until = TestUntil.happenings(1);
        listener = Mockito.mock(JournalListener.class);
        journal = Journal.using(world.stage(), PostgresJournalActor.class, listener, configuration);
        journal.registerEntryAdapter(TestEvent.class, new TestEventAdapter());
        journal.registerStateAdapter(Entity1.class, entity1Adapter);

        Mockito.doAnswer(x -> until.happened()).when(listener).appended(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAll(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedWith(any(), any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAllWith(any(), any());

        journalReader = journal.journalReader(streamName).await();
        streamReader = journal.streamReader(streamName).await();
    }

    @Test
    public void testThatInsertsANewEvent() {
        TestEvent appendedEvent = newEventForData(1);
        journal.append(streamName, 1, appendedEvent, interest, object);
        until.completes();

        Entry<String> entry = journalReader.readNext().await();
        TestEvent event = gson.fromJson(entry.entryData, TestEvent.class);
        assertEquals(appendedEvent, event);
    }

    @Test
    public void testThatInsertsANewListOfEvents() {
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        journal.appendAll(streamName, 1, asList(appendedEvent1, appendedEvent2), interest, object);
        until.completes();

        List<Entry<String>> eventStream = journalReader.readNext(2).await();
        Entry<String> entry1 = eventStream.get(0);
        TestEvent event1 = gson.fromJson(entry1.entryData, TestEvent.class);
        assertEquals(appendedEvent1, event1);

        Entry<String> entry2 = eventStream.get(1);
        TestEvent event2 = gson.fromJson(entry2.entryData, TestEvent.class);
        assertEquals(appendedEvent2, event2);
    }

    @Test
    public void testThatInsertsANewEventWithASnapshot() {
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        Entity1 entity = new Entity1("1", 123);

        until = TestUntil.happenings(3);
        journal.appendWith(streamName, 1, appendedEvent1, null, interest, object);
        journal.appendWith(streamName, 2, appendedEvent2, null, interest, object);
        journal.appendWith(streamName, 3, appendedEvent3, entity, interest, object);
        until.completes();

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithASnapshot() {
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        TestEvent appendedEvent4 = newEventForData(4);
        Entity1 entity = new Entity1("1", 123);

        until = TestUntil.happenings(2);
        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, appendedEvent4), entity, interest, object);
        until.completes();

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatReturnsSameReaderForSameName() {
        final String name = UUID.randomUUID().toString();
        assertEquals(journal.journalReader(name).await(), journal.journalReader(name).await());
        assertEquals(journal.streamReader(name).await(), journal.streamReader(name).await());
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
      public TextState toRawState(Entity1 state, int stateVersion, Metadata metadata) {
        final String serialization = JsonSerialization.serialized(state);
        return new TextState(state.id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
      }
    }
}