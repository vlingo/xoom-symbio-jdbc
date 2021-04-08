// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import static io.vlingo.xoom.symbio.store.EntryReader.Beginning;
import static io.vlingo.xoom.symbio.store.EntryReader.End;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.UUID;

import io.vlingo.xoom.symbio.Entry;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.testkit.TestUntil;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.store.journal.JournalReader;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCJournalReaderActor.JDBCJournalReaderInstantiator;

public abstract class JDBCJournalReaderActorTest extends BaseJournalTest {
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
        JournalReader<TextEntry> journalReader = journalReader();

        insertEvent(1);
        insertEvent(2);


        Entry<String> entry1 = journalReader.readNext().await();
        assertEquals(1, parse(entry1).number);
        assertTrue(entry1.entryVersion() > 0);

        Entry<String> entry2 = journalReader.readNext().await();
        assertEquals(2, parse(entry2).number);
        assertTrue(entry2.entryVersion() > 0);

        // gap prevention check for one event
        assertNull(journalReader.readNext().await());
    }

    @Test
    public void testThatRetrievesFromSavedOffset() throws Exception {
        insertEvent(1);
        insertEvent(2);
        long offset = insertEvent(3);
        long lastOffset = insertEvent(4);

        insertOffset(offset, readerName);
        JournalReader<TextEntry> journalReader = journalReader();

        Entry<String> entry1 = journalReader.readNext().await();
        assertEquals(3, parse(entry1).number);
        assertTrue(entry1.entryVersion() > 0);

        Entry<String> entry2 = journalReader.readNext().await();
        assertEquals(4, parse(entry2).number);
        assertTrue(entry2.entryVersion() > 0);

        assertNotEquals(offset, lastOffset);
    }

    @Test
    public void testThatRetrievesInBatches() throws Exception {
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);

        JournalReader<TextEntry> journalReader = journalReader();
        List<TextEntry> events = journalReader.readNext(2).await();
        assertEquals(2, events.size());

        assertEquals(1, parse(events.get(0)).number);
        assertTrue(events.get(0).entryVersion() > 0);

        assertEquals(2, parse(events.get(1)).number);
        assertTrue(events.get(1).entryVersion() > 0);

        // gap prevention check for multiple entries
        events = journalReader.readNext(5).await();
        assertEquals(2, events.size());

        assertEquals(3, parse(events.get(0)).number);
        assertTrue(events.get(0).entryVersion() > 0);

        assertEquals(4, parse(events.get(1)).number);
        assertTrue(events.get(1).entryVersion() > 0);
    }

    @Test
    public void testThatRewindReadsFromTheBeginning() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        insertEvent(1);
        long offset = insertEvent(2);

        insertOffset(offset, readerName);
        JournalReader<TextEntry> journalReader = journalReader();
        journalReader.rewind();

        until.completesWithin(50);
        assertOffsetIs(readerName, 1);
        TextEntry event = journalReader.readNext().await();
        assertEquals(1, parse(event).number);
        assertTrue(event.entryVersion() > 0);
    }

    @Test
    public void testThatSeekToGoesToTheBeginningWhenSpecified() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        JournalReader<TextEntry> journalReader = journalReader();
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

        JournalReader<TextEntry> journalReader = journalReader();
        journalReader.seekTo(End).await();

        until.completesWithin(50);
        assertOffsetIs(readerName, lastOffset + 1);
    }

    @Test
    public void testThatDataVersionsGreaterThanZero() throws Exception {
        JournalReader<TextEntry> journalReader = journalReader();

        insertEvent(1);
        insertEvent(2);

        final TextEntry entry1 = journalReader.readNext().await();
        assertTrue(entry1.entryVersion() > 0);

        final TextEntry entry2 = journalReader.readNext().await();
        assertTrue(entry2.entryVersion() > 0);

        // gap prevention check for one event
        assertNull(journalReader.readNext().await());
    }

    @SuppressWarnings("unchecked")
    private JournalReader<TextEntry> journalReader() {
        return world.actorFor(
                JournalReader.class,
                Definition.has(JDBCJournalReaderActor.class,
                        new JDBCJournalReaderInstantiator(configuration, readerName))
        );
    }
}