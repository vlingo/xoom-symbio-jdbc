// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.yugabyte;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCStreamReaderActorTest;
import io.vlingo.xoom.symbio.store.testcontainers.SharedYugaByteDbContainer;
import org.junit.Ignore;

@Ignore
public class YugaByteStreamReaderActorTest extends JDBCStreamReaderActorTest {

  @Override
  protected Configuration.TestConfiguration testConfiguration(DataFormat format) {
    try {
      SharedYugaByteDbContainer dbContainer = SharedYugaByteDbContainer.getInstance();
      return dbContainer.testConfiguration(format);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create YugaByte test configuration because: " + e.getMessage(), e);
    }
  }
}
