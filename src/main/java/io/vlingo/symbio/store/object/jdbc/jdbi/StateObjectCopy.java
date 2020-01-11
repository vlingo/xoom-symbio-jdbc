// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
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

import io.vlingo.symbio.store.object.StateObject;

/**
 * A binary copy of a given {@code StateObject}.
 * <p>
 * This is not thread-safe and is intended for use by an
 * actor-based implementation where a single copy will
 * be made at any given time. Note that the {@code byteStream}
 * and {@code serializer} are intentionally static.
 * </p>
 */
final class StateObjectCopy {
  // TODO: Improve choice of serializer
  private static final ByteArrayOutputStream byteStream;
  private static final ObjectOutputStream serializer;

  static {
    try {
      byteStream = new ByteArrayOutputStream();
      serializer = new ObjectOutputStream(byteStream);
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize StateObjectCopy.");
    }
  }

  private final byte[] image;

  /**
   * Answer a new {@code Map<Long, StateObjectCopy>} from the {@code persistentObjects}.
   * @param persistentObjects the {@code List<?>}
   * @return {@code Map<Long, StateObjectCopy>}
   */
  static Map<Long,StateObjectCopy> all(final List<?> persistentObjects) {
    final Map<Long,StateObjectCopy> all = new HashMap<>(persistentObjects.size());
    for (final Object persistentObject : persistentObjects) {
      final StateObject typed = StateObject.from(persistentObject);
      all.put(typed.persistenceId(), new StateObjectCopy(typed));
    }
    return all;
  }

  /**
   * Answer a new {@code Map<Long, StateObjectCopy>} from the {@code persistentObject}.
   * @param persistentObject the StateObject
   * @return {@code Map<Long, StateObjectCopy>}
   */
  static Map<Long, StateObjectCopy> of(final StateObject persistentObject) {
    return Collections.singletonMap(persistentObject.persistenceId(), new StateObjectCopy(persistentObject));
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
   * @param persistentObject the {@code StateObject} to copy.
   */
  StateObjectCopy(final StateObject persistentObject) {
    this.image = toBytes(persistentObject);
  }

  /**
   * Answer whether or not this {@code image} differs from that of the {@code persistentObject}.
   * @param persistentObject the StateObject to compare
   * @return boolean
   */
  boolean differsFrom(final StateObject persistentObject) {
    final byte[] other = toBytes(persistentObject);
    return !Arrays.equals(image, other);
  }

  byte[] image() {
    return image;
  }

  @Override
  public String toString() {
    return "StateObjectCopy[image=" + readable(image) + "]";
  }

  /**
   * Answer the byte array of the serialized {@code persistentObject}.
   * @param persistentObject the StateObject to serialize
   * @return byte[]
   */
  private byte[] toBytes(final StateObject persistentObject) {
    try {
      byteStream.reset();
      serializer.reset();
      serializer.writeObject(persistentObject);
      return byteStream.toByteArray();
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize StateObjectCopy.");
    }
  }
}
