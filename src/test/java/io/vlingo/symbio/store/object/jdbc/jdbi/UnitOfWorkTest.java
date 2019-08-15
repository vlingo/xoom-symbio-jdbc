// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;

public class UnitOfWorkTest {

  @Test
  public void testThatUnitOfWorkInstantiates() {
    final UnitOfWork unitOfWork = UnitOfWork.acquireFor(100L, new Person("John Jones", 35, 1L));
    assertNotNull(unitOfWork);
    assertEquals(100L, unitOfWork.unitOfWorkId);
  }

  @Test
  public void testThatUnitOfWorkDetectsModifications() {
    final Person person = new Person("John Jones", 35, 1L);
    final UnitOfWork unitOfWork = UnitOfWork.acquireFor(100L, person);
    assertEquals(100L, unitOfWork.unitOfWorkId);
    assertFalse(unitOfWork.isModified(person));
    assertTrue(unitOfWork.isModified(person.withAge(36)));
    assertTrue(unitOfWork.isModified(person.withName("John Jones-Milton")));
  }

  @Test
  public void testThatUnitOfWorkDetectsListModifications() {
    final Person person1 = new Person("John Jones", 35, 1L);
    final Person person2 = new Person("John Doe", 31, 2L);
    final Person person3 = new Person("Zoe Jones-Doe", 28, 3L);

    final UnitOfWork unitOfWork = UnitOfWork.acquireFor(100L, Arrays.asList(person1, person2, person3));
    assertEquals(100L, unitOfWork.unitOfWorkId);
    assertFalse(unitOfWork.isModified(person1));
    assertTrue(unitOfWork.isModified(person1.withAge(36)));
    assertFalse(unitOfWork.isModified(person2));
    assertTrue(unitOfWork.isModified(person2.withName("John Jones-Doe")));
    assertFalse(unitOfWork.isModified(person3));
    assertTrue(unitOfWork.isModified(person3.withName("John Jones-Milton, III").withAge(37)));
  }

  @Test
  public void testThatUnitOfWorkTimesOut() {
    final UnitOfWork unitOfWork = UnitOfWork.acquireFor(100L, new Person("John Jones", 35, 1L));
    assertEquals(100L, unitOfWork.unitOfWorkId);
    final long pastTime1 = System.currentTimeMillis() - 5500L;
    assertFalse(unitOfWork.hasTimedOut(pastTime1));
    final long pastTime2 = System.currentTimeMillis() - 200L;
    assertFalse(unitOfWork.hasTimedOut(pastTime2));
    final long futureTime = System.currentTimeMillis() + 1L;
    assertTrue(unitOfWork.hasTimedOut(futureTime));
  }
}
