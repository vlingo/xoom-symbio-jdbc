// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.object.ObjectStoreDelegate;

/**
 * The {@code JDBCObjectStoreDelegate} abstract base used by
 * {@code JDBCObjectStoreActor} to interact with specific delegates,
 * and also extended by any number of those concrete delegates.
 */
public abstract class JDBCObjectStoreDelegate implements ObjectStoreDelegate<Entry<?>, State<?>> {
  public static enum Type { Jdbi, JDBC, JPA };

  public final Configuration configuration;

  /**
   * Constructs my default state.
   *
   * @param configuration the Configuration used to configure my concrete subclasses
   */
  protected JDBCObjectStoreDelegate(final Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Check for timed out transactions.
   */
  public abstract void timeoutCheck();

  /**
   * Answer my Type.
   * @return Type
   */
  public Type type() {
    return Type.JDBC;
  }
}
