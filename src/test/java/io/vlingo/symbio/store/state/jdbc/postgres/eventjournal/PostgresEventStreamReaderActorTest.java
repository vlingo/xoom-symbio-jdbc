package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import io.vlingo.actors.Definition;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventStream;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class PostgresEventStreamReaderActorTest extends BasePostgresEventJournalTest {
    private EventStreamReader<TestAggregateRoot> eventStreamReader;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        eventStreamReader = world.actorFor(
                Definition.has(PostgresEventStreamReaderActor.class,
                        Definition.parameters(configuration)),
                EventStreamReader.class
        );

        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);
    }

    @Test
    public void testThatCanReadAllEventsFromJournal() throws Exception {
        EventStream<TestAggregateRoot> stream = eventStreamReader.streamFor(streamName).await();
        assertEquals(State.NullState.Text, stream.snapshot);
        assertEquals(5, stream.streamVersion);
        assertEquals(stream.streamName, streamName);

        AtomicInteger eventNumber = new AtomicInteger(1);
        stream.events.forEach(event -> assertEquals(eventNumber.getAndIncrement(), event.eventData.number));
    }

    @Test
    public void testThatCanReadAllEventsFromJournalBasedOnOffsetReturnSnapshot() throws Exception {
        TestAggregateRoot snapshotState = new TestAggregateRoot(streamName, 2);
        insertSnapshot(2, snapshotState);

        EventStream<TestAggregateRoot> stream = eventStreamReader.streamFor(streamName, 1).await();
        assertEquals(2, stream.snapshot.dataVersion);
        assertEquals(snapshotState, stream.snapshot.data);
        assertEquals(stream.streamVersion, 5);
        assertEquals(stream.streamName, streamName);

        Assert.assertEquals(2, stream.events.size());
        Assert.assertEquals(3, stream.events.get(0).eventData.number);
        Assert.assertEquals(4, stream.events.get(1).eventData.number);
    }

    @Test
    public void testThatCanReadAllEventsFromJournalBasedOnOffsetDoesNotReturnSnapshotIfOffsetIsHigher() throws Exception {
        TestAggregateRoot snapshotState = new TestAggregateRoot(streamName, 1);
        insertSnapshot(1, snapshotState);

        EventStream<TestAggregateRoot> stream = eventStreamReader.streamFor(streamName, 4).await();
        assertEquals(State.NullState.Text, stream.snapshot);
        assertEquals(stream.streamVersion, 5);
        assertEquals(stream.streamName, streamName);

        Assert.assertEquals(1, stream.events.size());
        Assert.assertEquals(4, stream.events.get(0).eventData.number);
    }
}

