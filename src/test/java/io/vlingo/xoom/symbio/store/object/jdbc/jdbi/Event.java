// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import io.vlingo.xoom.symbio.Source;

public class Event extends Source<Event> {
  private String value;

  public Event(final String value) {
    super();
    this.value = value;
  }

  public Event(final int eventTypeVersion) {
    super(eventTypeVersion);
  }

  public String value() {
    return value;
  }

  @Override
  public int hashCode() {
    return 31 * value.hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }
    final Event otherEvent = (Event) other;

    return value.equals(otherEvent.value);
  }

  @Override
  public String toString() {
    return "[Event value=" + value + "]";
  }
}
