// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.EntryAdapter;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEvents.PersonAdded;
import io.vlingo.symbio.store.object.jdbc.jpa.PersonEvents.PersonRenamed;
import io.vlingo.symbio.store.object.jdbc.jpa.model.JPAEntry;

/**
 * PersonEntryAdapters
 */
public final class PersonEntryAdapters {

  /**
   * PersonAddedAdapter is responsible for adapting between {@link PersonAdded}
   * domain events and {@link JPAEntry} typed journal entries.
   */
  public static class PersonAddedAdapter implements EntryAdapter<PersonAdded, JPAEntry> {

    /* @see io.vlingo.symbio.EntryAdapter#fromEntry(io.vlingo.symbio.Entry) */
    @Override
    public PersonAdded fromEntry(JPAEntry entry) {
      return JsonSerialization.deserialized(entry.entryData(), PersonAdded.class);
    }


    /* @see io.vlingo.symbio.EntryAdapter#toEntry(io.vlingo.symbio.Source, io.vlingo.symbio.Metadata) */
    @Override
    public JPAEntry toEntry(final PersonAdded source, final Metadata metadata) {
      return new JPAEntry(PersonAdded.class, 1, JsonSerialization.serialized(source), metadata);
    }

    /* @see io.vlingo.symbio.EntryAdapter#toEntry(io.vlingo.symbio.Source, java.lang.String, io.vlingo.symbio.Metadata) */
    @Override
    public JPAEntry toEntry(final PersonAdded source, final String id, final Metadata metadata) {
      return new JPAEntry(PersonAdded.class, 1, JsonSerialization.serialized(source), metadata);
    }

    @Override
    public JPAEntry toEntry(final PersonAdded source, final int version, final String id, final Metadata metadata) {
      return new JPAEntry(PersonAdded.class, 1, JsonSerialization.serialized(source), version, metadata);
    }
  }

  /**
   * PersonRenamedAdapter is responsible for adapting between {@link PersonRenamed}
   * domain events and {@link JPAEntry} typed journal entries.
   */
  public static class PersonRenamedAdapter implements EntryAdapter<PersonRenamed, JPAEntry> {

    /* @see io.vlingo.symbio.EntryAdapter#fromEntry(io.vlingo.symbio.Entry) */
    @Override
    public PersonRenamed fromEntry(JPAEntry entry) {
      return JsonSerialization.deserialized(entry.entryData(), PersonRenamed.class);
    }

    /* @see io.vlingo.symbio.EntryAdapter#toEntry(io.vlingo.symbio.Source, io.vlingo.symbio.Metadata) */
    @Override
    public JPAEntry toEntry(final PersonRenamed source, final Metadata metadata) {
      return new JPAEntry(PersonRenamed.class, 1, JsonSerialization.serialized(source), metadata);
    }

    /* @see io.vlingo.symbio.EntryAdapter#toEntry(io.vlingo.symbio.Source, java.lang.String, io.vlingo.symbio.Metadata) */
    @Override
    public JPAEntry toEntry(final PersonRenamed source, final String id, final Metadata metadata) {
      return new JPAEntry(PersonRenamed.class, 1, JsonSerialization.serialized(source), metadata);
    }

    @Override
    public JPAEntry toEntry(final PersonRenamed source, final int version, final String id, final Metadata metadata) {
      return new JPAEntry(PersonRenamed.class, 1, JsonSerialization.serialized(source), version, metadata);
    }
  }
}
