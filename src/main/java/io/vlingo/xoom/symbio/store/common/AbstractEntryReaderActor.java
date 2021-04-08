// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.reactivestreams.Stream;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.store.EntryReaderStream;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.gap.GapRetryReader;
import io.vlingo.xoom.symbio.store.gap.GappedEntries;
import io.vlingo.xoom.symbio.store.journal.JournalReader;
import io.vlingo.xoom.symbio.store.state.StateStoreEntryReader;

public abstract class AbstractEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {
    private final Advice advice;
    private final String name;
    private final Configuration configuration;
    private final EntryAdapterProvider entryAdapterProvider;

    private final PreparedStatement queryBatch;
    private final String queryIds;
    private final PreparedStatement queryCount;
    private final PreparedStatement queryOne;
    private final PreparedStatement queryLatestOffset;
    private final PreparedStatement updateCurrentOffset;

    private GapRetryReader<T> reader = null;

    private long currentId = 0L;

    public AbstractEntryReaderActor(final Advice advice, final String name) throws Exception {
        this.advice = advice;
        this.name = name;
        this.configuration = advice.specificConfiguration();

        this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());

        try {
            this.queryBatch = configuration.connection.prepareStatement(this.advice.queryEntryBatchExpression);
            this.queryIds = this.advice.queryEntryIdsExpression;
            this.queryCount = configuration.connection.prepareStatement(this.advice.queryCount);
            this.queryLatestOffset = configuration.connection.prepareStatement(this.advice.queryLatestOffset);
            this.queryOne = configuration.connection.prepareStatement(this.advice.queryEntryExpression);
            this.updateCurrentOffset = configuration.connection.prepareStatement(this.advice.queryUpdateCurrentOffset);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Read one entry from the {@link ResultSet}.
     * @param result the ResultSet from which to read
     * @return T
     * @throws Exception if the read fails
     */
    protected abstract T entryFrom(final ResultSet result) throws Exception;

    @Override
    public void close() {
        try {
            queryBatch.close();
            queryOne.close();
            configuration.connection.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    @Override
    public Completes<String> name() {
        return completes().with(name);
    }

    @Override
    public Completes<T> readNext() {
        try {
            queryOne.clearParameters();
            queryOne.setLong(1, currentId);
            try (final ResultSet result = queryOne.executeQuery()) {
                if (result.first()) {
                    final T entry = entryFrom(result);

                    ++currentId;
                    return completes().with(entry);
                } else {
                    List<Long> gapIds = reader().detectGaps(null, currentId);
                    GappedEntries<T> gappedEntries = new GappedEntries<>(new ArrayList<>(), gapIds, completesEventually());
                    reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

                    ++currentId;
                    return completes();
                }
            }
        } catch (Exception e) {
            logger().error("Unable to read next entry for " + name + " because: " + e.getMessage(), e);
            return completes().with(null);
        }
    }

    @Override
    public Completes<List<T>> readNext(final int maximumEntries) {
        try {
            queryBatch.clearParameters();
            queryBatch.setLong(1, currentId);
            queryBatch.setInt(2, maximumEntries);
            try (final ResultSet result = queryBatch.executeQuery()) {
                final List<T> entries = mapQueriedEntriesFrom(result);
                List<Long> gapIds = reader().detectGaps(entries, currentId, maximumEntries);
                if (!gapIds.isEmpty()) {
                    GappedEntries<T> gappedEntries = new GappedEntries<>(entries, gapIds, completesEventually());
                    reader().readGaps(gappedEntries, DefaultGapPreventionRetries, DefaultGapPreventionRetryInterval, this::readIds);

                    // Move offset with maximumEntries regardless of filled up gaps
                    currentId += maximumEntries;
                    return completes();
                } else {
                    currentId += maximumEntries;
                    return completes().with(entries);
                }
            }
        } catch (Exception e) {
            logger().error("Unable to read next " + maximumEntries + " entries for " + name + " because: " + e.getMessage(), e);
            return completes().with(new ArrayList<>());
        }
    }

    @Override
    public Completes<List<T>> readNext(final String fromId, final int maximumEntries) {
        seekTo(fromId);
        return readNext(maximumEntries);
    }

    @Override
    public Completes<String> seekTo(final String id) {
        switch (id) {
            case Beginning:
                this.currentId = 1;
                updateCurrentOffset();
                break;
            case End:
                this.currentId = retrieveLatestOffset() + 1;
                updateCurrentOffset();
                break;
            case Query:
                break;
            default:
                this.currentId = Integer.parseInt(id);
                updateCurrentOffset();
                break;
        }

        return completes().with(String.valueOf(currentId));
    }

    @Override
    public Completes<T> readNext(final String fromId) {
        seekTo(fromId);
        return readNext();
    }

    @Override
    public void rewind() {
        currentId = 0;
    }

    @Override
    public Completes<Long> size() {
        try (final ResultSet resultSet = queryCount.executeQuery()) {
            if (resultSet.next()) {
                final long count = resultSet.getLong(1);
                return completes().with(count);
            }
        } catch (Exception e) {
            logger().error("vlingo/symbio-postgres: " + e.getMessage(), e);
            logger().error("vlingo/symbio-postgres: Rewinding the offset");
        }

        return completes().with(-1L);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<Stream> streamAll() {
        return completes().with(new EntryReaderStream<>(stage(), selfAs(JournalReader.class), entryAdapterProvider));
    }

    protected Configuration getConfiguration() {
        return configuration;
    }

    private List<T> mapQueriedEntriesFrom(final ResultSet result) throws Exception {
        final List<T> entries = new ArrayList<>();
        while (result.next()) {
            final T entry = entryFrom(result);
            entries.add(entry);
        }

        return entries;
    }

    private PreparedStatement prepareQueryByIdsStatement(String query, List<Long> ids) throws SQLException {
        PreparedStatement statement = configuration.connection.prepareStatement(query);
        for (int i = 0; i < ids.size(); i++) {
            // parameter index starts from 1
            statement.setLong(i + 1, ids.get(i));
        }

        return statement;
    }

    private String queryByIds(String queryTemplate, int idsCount) {
        String[] placeholderList = new String[idsCount];
        Arrays.fill(placeholderList, "?");
        String placeholders = String.join(", ", placeholderList);

        return MessageFormat.format(queryTemplate, placeholders);
    }

    private List<T> readIds(List<Long> ids) {
        String query = queryByIds(queryIds, ids.size());
        try (PreparedStatement statement = prepareQueryByIdsStatement(query, ids);
             ResultSet result = statement.executeQuery()) {
            return mapQueriedEntriesFrom(result);
        } catch (Exception e) {
            logger().error("vlingo/symbio-postgres error: " + e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    private GapRetryReader<T> reader() {
        if (reader == null) {
            reader = new GapRetryReader<>(stage(), scheduler());
        }

        return reader;
    }

    private long retrieveLatestOffset() {
        try {
            queryBatch.clearParameters();
            queryLatestOffset.setString(1, name);
            try (ResultSet resultSet = queryLatestOffset.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getLong(1);
                }
            }
        } catch (Exception e) {
            logger().error("vlingo/symbio-hsqldb: Could not retrieve latest offset, using current.");
        }

        return 0;
    }

    private void updateCurrentOffset() {
        try {
            updateCurrentOffset.clearParameters();
            updateCurrentOffset.setLong(1, currentId);
            updateCurrentOffset.setString(2, name);
            updateCurrentOffset.setLong(3, currentId);

            updateCurrentOffset.executeUpdate();
            configuration.connection.commit();
        } catch (Exception e) {
            logger().error("vlingo/symbio-hsqldb: Could not persist the offset. Will retry on next read.");
            logger().error("vlingo/symbio-hsqldb: " + e.getMessage(), e);
        }
    }
}
