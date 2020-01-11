// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.time.LocalDateTime;

import org.jdbi.v3.core.mapper.Nested;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.Dispatchable;

public final class PersistentDispatchable {
  private final String originatorId;
  private final Dispatchable<Entry<?>, State<?>> dispatchable;

  public PersistentDispatchable(final String originatorId, final Dispatchable<Entry<?>, State<?>> dispatchable) {
    this.originatorId = originatorId;
    this.dispatchable = dispatchable;
  }
  
  public String originatorId() {
    return this.originatorId;
  }

  public String id() {
    return dispatchable.id();
  }

  public LocalDateTime createdOn() {
    return dispatchable.createdOn();
  }

  @Nested
  public PersistentState state() {
    return dispatchable.state().map(PersistentState::new).orElse(null);
  }

  public String entries() {
    if (dispatchable.entries() != null && !dispatchable.entries().isEmpty()) {
      return JsonSerialization.serialized(dispatchable.entries());
    } else {
      return null;
    }
  }

  public static class PersistentState {
    private final State<?> state;

    public PersistentState(final State<?> state) {
      this.state = state;
    }

    public String id() {
      return state.id;
    }

    public String data() {
      return JsonSerialization.serialized(state.data);
    }

    public int dataVersion() {
      return state.dataVersion;
    }

    public String type(){
      return state.type;
    }

    public int typeVersion() {
      return state.typeVersion;
    }

    public String metadata(){
      return JsonSerialization.serialized(state.metadata);
    }
  }
}
