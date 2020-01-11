// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.google.gson.Gson;

import io.vlingo.actors.World;
import io.vlingo.common.Tuple2;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.event.TestEvent;
import io.vlingo.symbio.store.common.jdbc.Configuration;

public abstract class BasePostgresJournalTest {
    protected Configuration configuration;
    protected World world;
    protected String aggregateRootId;
    protected Gson gson;
    protected String streamName;
    protected IdentityGenerator identityGenerator;
    protected JDBCQueries queries;

    @Before
    public void setUpDatabase() throws Exception {
        aggregateRootId = UUID.randomUUID().toString();
        streamName = aggregateRootId;
        world = World.startWithDefaults("event-stream-tests");
        configuration = testConfiguration(DataFormat.Text);
        gson = new Gson();
        identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();

        queries = JDBCQueries.queriesFor(configuration.connection);
        dropDatabase();
        queries.createTables();
    }

    @After
    public void tearDownDatabase() throws Exception {
        dropDatabase();
        world.terminate();
    }

    private void dropDatabase() throws SQLException {
      queries.dropTables();
    }

    protected final long insertEvent(final int dataVersion) throws SQLException, InterruptedException {
        Thread.sleep(2);

        final Tuple2<PreparedStatement, Optional<String>> insert =
                queries.prepareInsertEntryQuery(
                        aggregateRootId.toString(),
                        dataVersion,
                        gson.toJson(new TestEvent(aggregateRootId, dataVersion)),
                        TestEvent.class.getName(),
                        1,
                        gson.toJson(Metadata.nullMetadata()));

        assert insert._1.executeUpdate() == 1;
        configuration.connection.commit();

        return queries.generatedKeyFrom(insert._1);
    }

    protected final void insertOffset(final long offset, final String readerName) throws SQLException {
        queries.prepareUpsertOffsetQuery(readerName, offset).executeUpdate();
        configuration.connection.commit();
    }

    protected final void insertSnapshot(final int dataVersion, final TestEvent state) throws SQLException {
      queries.prepareInsertSnapshotQuery(
              streamName,
              dataVersion,
              gson.toJson(state),
              dataVersion,
              TestEvent.class.getName(),
              1,
              gson.toJson(Metadata.nullMetadata()))
              ._1
              .executeUpdate();

      configuration.connection.commit();
    }

    protected final void assertOffsetIs(final String readerName, final long offset) throws SQLException {
        final PreparedStatement select = queries.prepareSelectCurrentOffsetQuery(readerName);

        try (ResultSet resultSet = select.executeQuery()) {
          if (resultSet.next()) {
              final long currentOffset = resultSet.getLong(1);
              assertEquals(offset, currentOffset);
          } else {
            Assert.fail("Could not find offset for " + readerName);
          }
        }
    }

    protected final TestEvent parse(Entry<String> event) {
        return gson.fromJson(event.entryData(), TestEvent.class);
    }

    protected abstract Configuration.TestConfiguration testConfiguration(final DataFormat format) throws Exception;
}
