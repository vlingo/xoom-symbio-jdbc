// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.hsqldb;

import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.symbio.BaseEntry.BinaryEntry;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.common.AbstractEntryReaderActor;

import java.sql.Blob;
import java.sql.ResultSet;

public class HSQLDBStateStoreEntryReaderActor<T extends Entry<?>> extends AbstractEntryReaderActor<T> {
  public HSQLDBStateStoreEntryReaderActor(final Advice advice, final String name) throws Exception {
    super(advice, name);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected T entryFrom(final ResultSet result) throws Exception {
    final long id = result.getLong(1);
    final String type = result.getString(2);
    final int typeVersion = result.getInt(3);
    final String metadataValue = result.getString(5);
    final String metadataOperation = result.getString(6);
    final int entryVersion = result.getInt(7); // from E_ENTRY_VERSION

    final Metadata metadata = Metadata.with(metadataValue, metadataOperation);

    if (getConfiguration().format.isBinary()) {
      return (T) new BinaryEntry(String.valueOf(id), typed(type), typeVersion, binaryDataFrom(result, 4), entryVersion, metadata);
    } else {
      return (T) new TextEntry(String.valueOf(id), typed(type), typeVersion, textDataFrom(result, 4), entryVersion, metadata);
    }
  }

  private byte[] binaryDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final Blob blob = resultSet.getBlob(columnIndex);
    final byte[] data = blob.getBytes(1, (int) blob.length());
    return data;
  }

  private String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final String data = resultSet.getString(columnIndex);
    return data;
  }

  private Class<?> typed(final String typeName) throws Exception {
    return StoredTypes.forName(typeName);
  }


  public static class HSQLDBStateStoreEntryReaderInstantiator<T extends Entry<?>> implements ActorInstantiator<HSQLDBStateStoreEntryReaderActor<T>> {
    private static final long serialVersionUID = -4281215905570289673L;

    private Advice advice;
    private String name;

    public HSQLDBStateStoreEntryReaderInstantiator() { }

    @Override
    public HSQLDBStateStoreEntryReaderActor<T> instantiate() {
      try {
        return new HSQLDBStateStoreEntryReaderActor<>(advice, name);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to instantiate " + getClass() + " because: " + e.getMessage(), e);
      }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Class<HSQLDBStateStoreEntryReaderActor<T>> type() {
      return (Class) HSQLDBStateStoreEntryReaderActor.class;
    }

    @Override
    public void set(final String name, final Object value) {
      switch (name) {
      case "advice":
        this.advice = (Advice) value;
        break;
      case "name":
        this.name = (String) value;
        break;
      }
    }
  }
}
