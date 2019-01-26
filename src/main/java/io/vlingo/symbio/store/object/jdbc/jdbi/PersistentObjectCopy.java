// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.symbio.store.object.PersistentObject;

/**
 * A binary copy of a given {@code PersistentObject}.
 * <p>
 * This is not thread-safe and is intended for use by an
 * actor-based implementation where a single copy will
 * be made at any given time. Note that the {@code byteStream}
 * and {@code serializer} are intentionally static.
 * </p>
 */
final class PersistentObjectCopy {
  // TODO: Improve choice of serializer
  private static final ByteArrayOutputStream byteStream;
  private static final ObjectOutputStream serializer;

  static {
    try {
      byteStream = new ByteArrayOutputStream();
      serializer = new ObjectOutputStream(byteStream);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize PersistentObjectCopy.");
    }
  }

  private final byte[] image;

  /**
   * Answer a new {@code Map<Long, PersistentObjectCopy>} from the {@code persistentObjects}.
   * @param persistentObjects the {@code List<?>}
   * @return {@code Map<Long, PersistentObjectCopy>}
   */
  static Map<Long,PersistentObjectCopy> all(final List<?> persistentObjects) {
    final Map<Long,PersistentObjectCopy> all = new HashMap<>(persistentObjects.size());
    for (final Object persistentObject : persistentObjects) {
      final PersistentObject typed = PersistentObject.from(persistentObject);
      all.put(typed.persistenceId(), new PersistentObjectCopy(typed));
    }
    return all;
  }

  /**
   * Answer a new {@code Map<Long, PersistentObjectCopy>} from the {@code persistentObject}.
   * @param persistentObject the PersistentObject
   * @return {@code Map<Long, PersistentObjectCopy>}
   */
  static Map<Long, PersistentObjectCopy> of(final PersistentObject persistentObject) {
    return Collections.singletonMap(persistentObject.persistenceId(), new PersistentObjectCopy(persistentObject));
  }

  static String readable(byte[] data) {
    final StringBuilder builder = new StringBuilder();
    for (int idx = 0; idx < data.length; ++idx) {
      builder.append("[").append(data[idx]).append("]");
    }
    return builder.toString();
  }

  /**
   * Constructs my state from the {@code persistentObject}.
   * @param persistentObject the {@code PersistentObject} to copy.
   */
  PersistentObjectCopy(final PersistentObject persistentObject) {
    this.image = toBytes(persistentObject);
  }

  /**
   * Answer whether or not this {@code image} differs from that of the {@code persistentObject}.
   * @param persistentObject the PersistentObject to compare
   * @return boolean
   */
  boolean differsFrom(final PersistentObject persistentObject) {
    final byte[] other = toBytes(persistentObject);
    return !Arrays.equals(image, other);
  }

  byte[] image() {
    return image;
  }

  @Override
  public String toString() {
    return "PersistentObjectCopy[image=" + readable(image) + "]";
  }

  /**
   * Answer the byte array of the serialized {@code persistentObject}.
   * @param persistentObject the PersistentObject to serialize
   * @return byte[]
   */
  private byte[] toBytes(final PersistentObject persistentObject) {
    try {
      byteStream.reset();
      serializer.reset();
      serializer.writeObject(persistentObject);
      return byteStream.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize PersistentObjectCopy.");
    }
  }
}
