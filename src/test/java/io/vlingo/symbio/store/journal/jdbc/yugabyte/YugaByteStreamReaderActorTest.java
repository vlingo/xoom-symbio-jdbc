// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.yugabyte;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.journal.jdbc.JDBCStreamReaderActorTest;
import io.vlingo.symbio.store.testcontainers.SharedYugaByteDbContainer;
import org.junit.Ignore;

@Ignore
public class YugaByteStreamReaderActorTest extends JDBCStreamReaderActorTest {
    private SharedYugaByteDbContainer dbContainer = SharedYugaByteDbContainer.getInstance();

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return dbContainer.testConfiguration(format);
    }
}
