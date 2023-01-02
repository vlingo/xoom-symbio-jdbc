// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.hsqldb;

import static io.vlingo.xoom.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider.testConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import io.vlingo.xoom.symbio.store.state.jdbc.QueryResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State.BinaryState;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration.TestConfiguration;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;

public class HSQLDBStorageDelegateTest {
  private TestConfiguration configuration;
  private HSQLDBStorageDelegate delegate;
  private String entity1StoreName;
  private World world;

  @Test
  public void testThatDatabaseOpensTablesCreated() throws Exception  {
    configuration = testConfiguration(DataFormat.Text);
    delegate = new HSQLDBStorageDelegate(configuration, world.defaultLogger());

    assertNotNull(delegate);
  }

  @Test
  public void testThatTextWritesRead() throws Exception {
    configuration = testConfiguration(DataFormat.Text);
    delegate = new HSQLDBStorageDelegate(configuration, world.defaultLogger());

    assertNotNull(delegate);

    final TextState writeState = new TextState("123", Entity1.class, 1, "data", 1, Metadata.with("metadata", "op"));
    
    delegate.beginWrite();
    final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
    writeStatement.executeUpdate();
    delegate.complete();

    delegate.beginRead();
    final QueryResource queryResource = delegate.createReadExpressionFor(entity1StoreName, "123");
    final ResultSet resultSet = queryResource.execute();
    final TextState readState = delegate.stateFrom(resultSet, "123");
    resultSet.close();
    queryResource.close();
    delegate.complete();

    assertEquals(writeState, readState);
  }

  @Test
  public void testThatTextStatesUpdate() throws Exception {
    configuration = testConfiguration(DataFormat.Text);
    delegate = new HSQLDBStorageDelegate(configuration, world.defaultLogger());

    assertNotNull(delegate);

    final TextState writeState1 = new TextState("123", Entity1.class, 1, "data1", 1, Metadata.with("metadata1", "op1"));
    
    delegate.beginWrite();
    final PreparedStatement writeStatement1 = delegate.writeExpressionFor(entity1StoreName, writeState1);
    writeStatement1.executeUpdate();
    delegate.complete();

    delegate.beginRead();
    final QueryResource queryResource1 = delegate.createReadExpressionFor(entity1StoreName, "123");
    final ResultSet resultSet1 = queryResource1.execute();
    final TextState readState1 = delegate.stateFrom(resultSet1, "123");
    resultSet1.close();
    queryResource1.close();
    delegate.complete();

    assertEquals(writeState1, readState1);

    final TextState writeState2 = new TextState("123", Entity1.class, 1, "data2", 1, Metadata.with("metadata2", "op2"));

    delegate.beginWrite();
    final PreparedStatement writeStatement2 = delegate.writeExpressionFor(entity1StoreName, writeState2);
    writeStatement2.executeUpdate();
    delegate.complete();
    
    delegate.beginRead();
    final QueryResource queryResource2 = delegate.createReadExpressionFor(entity1StoreName, "123");
    final ResultSet resultSet2 = queryResource2.execute();
    final TextState readState2 = delegate.stateFrom(resultSet2, "123");
    resultSet2.close();
    queryResource2.close();
    delegate.complete();

    assertEquals(writeState2, readState2);
    assertNotEquals(0, writeState1.compareTo(readState2));
    assertNotEquals(0, writeState2.compareTo(readState1));
  }

  @Test
  public void testThatBinaryWritesRead() throws Exception {
    configuration = testConfiguration(DataFormat.Binary);
    delegate = new HSQLDBStorageDelegate(configuration, world.defaultLogger());

    assertNotNull(delegate);

    final BinaryState writeState = new BinaryState("123", Entity1.class, 1, "data".getBytes(), 1, Metadata.with("metadata", "op"));
    
    delegate.beginWrite();
    final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
    writeStatement.executeUpdate();
    delegate.complete();

    delegate.beginRead();
    final QueryResource queryResource = delegate.createReadExpressionFor(entity1StoreName, "123");
    final ResultSet resultSet = queryResource.execute();
    final BinaryState readState = delegate.stateFrom(resultSet, "123");
    resultSet.close();
    queryResource.close();
    delegate.complete();

    assertEquals(writeState, readState);
  }

  @Before
  public void setUp() {
    world = World.startWithDefaults("test-store");

    entity1StoreName = Entity1.class.getSimpleName();
    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);
  }

  @After
  public void tearDown() throws Exception {
    configuration.cleanUp();
    delegate.close();
    world.terminate();
  }
}
