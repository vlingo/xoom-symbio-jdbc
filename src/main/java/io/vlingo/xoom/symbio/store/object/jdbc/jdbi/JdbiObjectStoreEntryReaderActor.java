// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.jdbi.v3.core.mapper.RowMapper;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.EntryReaderStream;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.xoom.symbio.store.object.StateObjectMapper;

/**
 * An {@code ObjectStoreEntryReader} for Jdbi.
 */
public class JdbiObjectStoreEntryReaderActor extends Actor implements ObjectStoreEntryReader<Entry<String>> {
  private final JdbiPersistMapper currentEntryOffsetMapper;
  private final EntryAdapterProvider entryAdapterProvider;
  private final JdbiOnDatabase jdbi;
  private final String name;
  private GapRetryReader<Entry<String>> reader = null;
  private final QueryExpression queryLastEntryId;
  private final QueryExpression querySize;

  private long offset;

  public JdbiObjectStoreEntryReaderActor(final JdbiOnDatabase jdbi, final Collection<StateObjectMapper> mappers, final String name) {
    this.jdbi = jdbi;
    this.name = name;
    this.offset = 1L;
    this.queryLastEntryId = jdbi.queryLastEntryId();
    this.currentEntryOffsetMapper = jdbi.currentEntryOffsetMapper(new String[] {":name", ":offset"});
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.querySize = jdbi.querySize();

    mappers.forEach(mapper -> jdbi.handle.registerRowMapper((RowMapper<?>) mapper.queryMapper()));

    restoreCurrentOffset();
  }

  @Override
  public void close() {
    if (!jdbi.isClosed()) {
      jdbi.close();
    }
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Completes<Entry<String>> readNext() {
    try {
      final QueryExpression expression = jdbi.queryEntry(offset);
      final Optional<Entry> entry = jdbi.handle().createQuery(expression.query).mapTo(Entry.class).findOne();
      List<Long> gapIds = reader().detectGaps(entry.orElse(null), offset);
      if (!gapIds.isEmpty()) {
        // gaps have been detected
        List<Entry<String>> entries = entry.isPresent() ? Collections.singletonList(entry.get()) : new ArrayList<>();
        GappedEntries<Entry<String>> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
        reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

        ++offset;
        updateCurrentOffset();
        return completes();
      } else {
        ++offset;
        updateCurrentOffset();
        return completes().with(entry.get());
      }
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<Entry<String>> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Completes<List<Entry<String>>> readNext(final int maximumEntries) {
    try {
      final QueryExpression expression = jdbi.queryEntries(offset, maximumEntries);
      final List<Entry<String>> entries = (List) jdbi.handle().createQuery(expression.query).mapTo(expression.type).list();
      List<Long> gapIds = reader().detectGaps(entries, offset, maximumEntries);
      if (!gapIds.isEmpty()) {
        GappedEntries<Entry<String>> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
        reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

        // Move offset with maximumEntries regardless of filled up gaps
        offset += maximumEntries;
        updateCurrentOffset();
        return completes();
      } else {
        offset += maximumEntries;
        updateCurrentOffset();
        return completes().with(entries);
      }
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read ids because: " + e.getMessage(), e);
      return completes().with(null);
    }
  }

  @Override
  public Completes<List<Entry<String>>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {
    this.offset = 1;
    updateCurrentOffset();
  }

  @Override
  public Completes<String> seekTo(final String id) {
    switch (id) {
    case Beginning:
        this.offset = 1;
        updateCurrentOffset();
        break;
    case End:
        this.offset = retrieveLatestOffset() + 1;
        updateCurrentOffset();
        break;
    case Query:
        break;
    default:
        this.offset = Long.parseLong(id);
        updateCurrentOffset();
        break;
    }

    return completes().with(String.valueOf(offset));
  }

  @Override
  public Completes<Long> size() {
    try {
      return completes().with(jdbi.handle().createQuery(querySize.query).mapTo(Long.class).one());
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve size, using -1L.");
      return completes().with(-1L);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<Stream> streamAll() {
    return completes().with(new EntryReaderStream<>(stage(), selfAs(EntryReader.class), entryAdapterProvider));
  }

  private GapRetryReader<Entry<String>> reader() {
    if (reader == null) {
      reader = new GapRetryReader<>(stage(), scheduler());
    }

    return reader;
  }


  @SuppressWarnings({ "rawtypes", "unchecked" })
  private List<Entry<String>> readIds(List<Long> ids) {
    try {
      final QueryExpression expression = jdbi.queryEntries(ids);
      final List<Entry<String>> entries = (List)jdbi.handle().createQuery(expression.query).mapTo(expression.type).list();
      return entries;
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not read next entry because: " + e.getMessage(), e);
      return new ArrayList<>();
    }
  }

  private void restoreCurrentOffset() {
    this.offset = retrieveLatestOffset();
  }

  private long retrieveLatestOffset() {
    try {
      return jdbi.handle().createQuery(queryLastEntryId.query).mapTo(Long.class).one();
    } catch (Exception e) {
      logger().info("xoom-symbio-jdbc: " + getClass().getSimpleName() + " Could not retrieve latest offset, using current.");
      return offset;
    }
  }

  private void updateCurrentOffset() {
    jdbi.handle().createUpdate(currentEntryOffsetMapper.insertStatement).bind("name", name).bind("offset", offset).execute();
  }

  public static class JdbiObjectStoreEntryReaderInstantiator implements ActorInstantiator<JdbiObjectStoreEntryReaderActor> {
    private static final long serialVersionUID = 3588678272821601213L;

    private final JdbiOnDatabase jdbi;
    final Collection<StateObjectMapper> mappers;
    final String name;

    public JdbiObjectStoreEntryReaderInstantiator(final JdbiOnDatabase jdbi, final Collection<StateObjectMapper> mappers, final String name) {
      this.jdbi = jdbi;
      this.mappers = mappers;
      this.name = name;
    }

    @Override
    public JdbiObjectStoreEntryReaderActor instantiate() {
      return new JdbiObjectStoreEntryReaderActor(jdbi, mappers, name);
    }

    @Override
    public Class<JdbiObjectStoreEntryReaderActor> type() {
      return JdbiObjectStoreEntryReaderActor.class;
    }
  }
}
