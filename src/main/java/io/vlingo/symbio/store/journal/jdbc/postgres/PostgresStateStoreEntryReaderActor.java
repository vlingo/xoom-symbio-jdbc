// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

public class PostgresStateStoreEntryReaderActor<T> extends Actor implements StateStoreEntryReader<T> {
  private final String name;

  public PostgresStateStoreEntryReaderActor(final String name) {
    this.name = name;
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<Entry<T>> readNext() {
    return null;
  }

  @Override
  public Completes<List<Entry<T>>> readNext(int maximumEntries) {
    return null;
  }

  @Override
  public void rewind() {

  }

  @Override
  public Completes<String> seekTo(String id) {
    return null;
  }
}
