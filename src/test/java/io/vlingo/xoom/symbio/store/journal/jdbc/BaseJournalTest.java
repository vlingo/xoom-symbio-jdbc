// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import com.google.gson.Gson;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.common.identity.IdentityGenerator;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.event.TestEvent;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public abstract class BaseJournalTest {
  protected Configuration.TestConfiguration configuration;
  protected Connection connection;
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
    connection = configuration.connectionProvider.newConnection();
    gson = new Gson();
    identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();
    // configuration = testConfiguration(DataFormat.Text);
    queries = JDBCQueries.queriesFor(connection);
    // dropDatabase();
    queries.createTables(connection);
  }

  @After
  public void tearDownDatabase() throws Exception {
    dropDatabase();
    connection.close(); // connection gets back to connection pool
    configuration.cleanUp();
    world.terminate();
  }

  private void dropDatabase() throws SQLException {
    queries.dropTables(connection);
  }

  protected final long insertEvent(final int dataVersion) throws SQLException, InterruptedException {
    Thread.sleep(2);

    final Tuple2<PreparedStatement, Optional<String>> insert =
        queries.prepareInsertEntryQuery(
            connection,
            aggregateRootId.toString(),
            dataVersion,
            gson.toJson(new TestEvent(aggregateRootId, dataVersion)),
            TestEvent.class.getName(),
            1,
            gson.toJson(Metadata.nullMetadata()));

    assert insert._1.executeUpdate() == 1;
    connection.commit();

    return queries.generatedKeyFrom(insert._1);
  }

  protected final void insertOffset(final long offset, final String readerName) throws SQLException {
    queries.prepareUpsertOffsetQuery(connection, readerName, offset).executeUpdate();
    connection.commit();
  }

  protected final void insertSnapshot(final int dataVersion, final TestEvent state) throws SQLException {
    queries.prepareInsertSnapshotQuery(
        connection,
        streamName,
        dataVersion,
        gson.toJson(state),
        dataVersion,
        TestEvent.class.getName(),
        1,
        gson.toJson(Metadata.nullMetadata()))
        ._1
        .executeUpdate();

    connection.commit();
  }

  protected final void assertOffsetIs(final String readerName, final long offset) throws SQLException {
    try (PreparedStatement select = queries.prepareSelectCurrentOffsetQuery(connection, readerName);
         ResultSet resultSet = select.executeQuery()) {
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

  protected abstract Configuration.TestConfiguration testConfiguration(final DataFormat format);
}
