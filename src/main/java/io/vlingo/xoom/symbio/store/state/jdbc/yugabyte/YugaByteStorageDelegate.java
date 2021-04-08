// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.yugabyte;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.state.jdbc.postgres.PostgresStorageDelegate;

public class YugaByteStorageDelegate extends PostgresStorageDelegate {
    public YugaByteStorageDelegate(Configuration configuration, Logger logger) {
        super(configuration, logger);
    }
}
