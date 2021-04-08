// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.common.event;

import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.EntryAdapter;
import io.vlingo.xoom.symbio.Metadata;

public final class TestEventAdapter implements EntryAdapter<TestEvent,TextEntry> {
  @Override
  public TestEvent fromEntry(final TextEntry entry) {
    return JsonSerialization.deserialized(entry.entryData(), TestEvent.class);
  }

  @Override
  public TextEntry toEntry(final TestEvent source, final Metadata metadata) {
    return toEntry(source, source.id, metadata);
  }

  @Override
  public TextEntry toEntry(final TestEvent source, final String id, final Metadata metadata) {
    final String serialization = JsonSerialization.serialized(source);
    return new TextEntry(TestEvent.class, 1, serialization, metadata);
  }

  @Override
  public TextEntry toEntry(final TestEvent source, final int version, final String id, final Metadata metadata) {
    final String serialization = JsonSerialization.serialized(source);
    return new TextEntry(TestEvent.class, 1, serialization, version, metadata);
  }
}
