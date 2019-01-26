// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import io.vlingo.symbio.store.object.PersistentObject;

public class Person extends PersistentObject {
  private static final long serialVersionUID = 1L;

  public final int age;
  public final long id;
  public final String name;

  public Person(final String name, final int age) {
    this(name, age, 1L);
  }

  public Person(final String name, final int age, final long persistenceId) {
    super(persistenceId);
    this.name = name;
    this.age = age;
    this.id = persistenceId;
  }

  public Person withAge(final int age) {
    return new Person(name, age, id);
  }

  public Person withName(final String name) {
    return new Person(name, age, id);
  }

  @Override
  public int hashCode() {
    return 31 * name.hashCode() * age * (int) persistenceId();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || other.getClass() != getClass()) {
      return false;
    }

    final Person otherPerson = (Person) other;

    return persistenceId() == otherPerson.persistenceId() && name.equals(otherPerson.name) && age == otherPerson.age;
  }

  @Override
  public String toString() {
    return "Person[id=" + id + " name=" + name + " age=" + age + "]";
  }
}
