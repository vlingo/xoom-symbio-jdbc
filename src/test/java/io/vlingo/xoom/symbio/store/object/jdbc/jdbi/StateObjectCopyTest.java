// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.junit.Test;

public class StateObjectCopyTest {
  @Test
  public void testThatObjectSerializes() {
    final Person person1 = new Person("Ben Charleston", 21, 1L);
    final Map<Long,StateObjectCopy> copy1 = StateObjectCopy.of(person1);
    assertNotNull(copy1);
    assertEquals(1, copy1.size());
    assertNotNull(copy1.get(1L));
    final Person person2 = new Person("Ben Charleston", 21, 1L);
    assertFalse(copy1.get(1L).differsFrom(person2));
  }

  @Test
  public void testThatModifiedObjectsDiffer() {
    final Person person1 = new Person("Ben Charleston", 21, 1L);
    final Map<Long,StateObjectCopy> copy1 = StateObjectCopy.of(person1);
    final Person person2 = new Person("Ben Charleston", 22, 1L);
    assertTrue(copy1.get(1L).differsFrom(person2));
  }

  @Test
  public void testThatModifiedListObjectsDiffer() {
    final Person person1 = new Person("Ben Charleston", 21, 1L);
    final Person person2 = new Person("Lucy Marcus", 25, 2L);
    final Person person3 = new Person("Jack Jones", 30, 3L);
    final Map<Long,StateObjectCopy> copy1 = StateObjectCopy.all(Arrays.asList(person1, person2, person3));
    final Person person1_2 = new Person("Ben Charleston", 22, 1L);
    assertTrue(copy1.get(person1_2.persistenceId()).differsFrom(person1_2));
    final Person person2_2 = new Person("Lucy Marqus", 25, 2L);
    assertTrue(copy1.get(person2_2.persistenceId()).differsFrom(person2_2));
    final Person person3_2 = new Person("Jack \"Jonesy\" Jones", 30, 3L);
    assertTrue(copy1.get(person3_2.persistenceId()).differsFrom(person3_2));
  }
}
