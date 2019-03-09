// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Objects;

/**
 * Person
 *
 */
public class Person implements ReferenceObject {

  protected int age;
  protected long id;
  protected String name;
  protected int version;

  public Person() {
    super();
  }

  public Person(long persistentId) {
    super();
    this.id = persistentId;
  }

  public Person(long persistentId, int anAge, String aName, int v) {
    this(persistentId);
    this.age = anAge;
    this.name = aName;
    this.version = v;
  }

  public Person(long persistentId, int anAge, String aName) {
    this(persistentId, anAge, aName, 0);
  }

  public Person newPersonWithAge(final int _age) {
    return new Person(this.id, _age, name, version);
  }

  public Person newPersonWithName(final String _name) {
    return new Person(this.id, this.age, _name, version);
  }

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  /* @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Person)) {
      return false;
    }
    Person other = (Person) obj;
    return id == other.id;
  }

  public static Person newPersonFrom(Person person) {
    return new Person(person.id, person.age, person.name, person.version);
  }

  /* @see io.vlingo.symbio.store.object.jdbc.jpa.ReferenceObject#id() */
  @Override
  @SuppressWarnings("unchecked")
  public Long id() {
    return this.id;
  }

}
