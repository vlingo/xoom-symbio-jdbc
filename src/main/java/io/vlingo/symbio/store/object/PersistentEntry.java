// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object;

import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;

@SuppressWarnings("rawtypes")
public class PersistentEntry extends StateObject implements Entry {
  private static final long serialVersionUID = 1L;

  private final Entry entry;

  public PersistentEntry(final Entry entry) {
    this.entry = entry;
  }

  public Source asSource(final EntryAdapterProvider provider) {
    return provider.asSource(entry);
  }

  public Entry entry() {
    return entry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public int compareTo(final Object other) {
    if (other instanceof PersistentEntry) {
      return entry.compareTo(other);
    }
    throw new IllegalArgumentException("Must be PersistentEntry");
  }

  @Override
  public String id() {
    return entry.id();
  }

  @Override
  public int entryVersion() {
    return entry.entryVersion();
  }

  @Override
  public Object entryData() {
    return entry.entryData();
  }

  @Override
  public Metadata metadata() {
    return entry.metadata();
  }

  @Override
  public String typeName() {
    return entry.typeName();
  }

  @Override
  public int typeVersion() {
    return entry.typeVersion();
  }

  @Override
  public boolean hasMetadata() {
    return entry.hasMetadata();
  }

  @Override
  public boolean isEmpty() {
    return entry.isEmpty();
  }

  @Override
  public boolean isNull() {
    return entry.isNull();
  }

  @Override
  public Class typed() {
    return entry.typed();
  }

  @Override
  public Entry withId(final String id) {
    return entry.withId(id);
  }

  @Override
  public int hashCode() {
    return entry.hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null) return false;
    if (this == other) return true;
    if (other.getClass() == Entry.class) return entry.equals(other);

    return entry.equals(other);
  }

  @Override
  public String toString() {
    return entry.toString();
  }

  protected PersistentEntry() {
    this.entry = null;
  }
}
