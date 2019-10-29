// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.yugabyte;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.state.jdbc.postgres.PostgresStorageDelegate;

public class YugaByteStorageDelegate extends PostgresStorageDelegate {
    public YugaByteStorageDelegate(Configuration configuration, Logger logger) {
        super(configuration, logger);
    }
}
