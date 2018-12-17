package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.actors.Definition;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.jdbc.postgres.PostgresJournalReaderActor;

import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static io.vlingo.symbio.store.journal.JournalReader.Beginning;
import static io.vlingo.symbio.store.journal.JournalReader.End;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PostgresJournalReaderActorTest extends BasePostgresJournalTest {
    private String readerName;

    @Before
    public void setUp() {
        readerName = UUID.randomUUID().toString();
    }

    @Test
    public void testThatReturnsCorrectName() {
        String name = journalReader().name().await();
        assertEquals(readerName, name);
    }

    @Test
    public void testThatRetrievesNextEvents() throws Exception {
        JournalReader<String> journalReader = journalReader();

        insertEvent(1);
        insertEvent(2);

        assertEquals(1, parse(journalReader.readNext().await()).number);
        assertEquals(2, parse(journalReader.readNext().await()).number);
    }

    @Test
    public void testThatRetrievesFromSavedOffset() throws Exception {
        insertEvent(1);
        insertEvent(2);
        long offset = insertEvent(3);
        long lastOffset = insertEvent(4);

        insertOffset(offset, readerName);
        JournalReader<String> journalReader = journalReader();

        assertEquals(3, parse(journalReader.readNext().await()).number);
        assertEquals(4, parse(journalReader.readNext().await()).number);
        assertNotEquals(offset, lastOffset);
    }

    @Test
    public void testThatRetrievesInBatches() throws Exception {
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);

        JournalReader<String> journalReader = journalReader();
        Stream<String> events = journalReader.readNext(2).await();
        assertEquals(2, events.entries.size());
        assertEquals(1, parse(events.entries.get(0)).number);
        assertEquals(2, parse(events.entries.get(1)).number);

        events = journalReader.readNext(2).await();
        assertEquals(2, events.entries.size());
        assertEquals(3, parse(events.entries.get(0)).number);
        assertEquals(4, parse(events.entries.get(1)).number);
    }

    @Test
    public void testThatRewindReadsFromTheBeginning() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        insertEvent(1);
        long offset = insertEvent(2);

        insertOffset(offset, readerName);
        JournalReader<String> journalReader = journalReader();
        journalReader.rewind();

        until.completesWithin(50);
        assertOffsetIs(readerName, 1);
        Entry<String> event = journalReader.readNext().await();
        assertEquals(1, parse(event).number);
    }

    @Test
    public void testThatSeekToGoesToTheBeginningWhenSpecified() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        JournalReader<String> journalReader = journalReader();
        journalReader.seekTo(Beginning).await();

        until.completesWithin(50);
        assertOffsetIs(readerName, 1);
    }

    @Test
    public void testThatSeekToGoesToTheEndWhenSpecified() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        insertEvent(1);
        insertEvent(2);
        long lastOffset = insertEvent(3);

        JournalReader<String> journalReader = journalReader();
        journalReader.seekTo(End).await();

        until.completesWithin(50);
        assertOffsetIs(readerName, lastOffset + 1);
    }

    @SuppressWarnings("unchecked")
    private JournalReader<String> journalReader() {
        return world.actorFor(
                Definition.has(PostgresJournalReaderActor.class,
                        Definition.parameters(configuration, readerName)),
                JournalReader.class
        );
    }
}