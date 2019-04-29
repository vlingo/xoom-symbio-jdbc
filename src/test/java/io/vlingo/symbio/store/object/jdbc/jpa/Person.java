// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import io.vlingo.symbio.store.object.PersistentObject;
/**
 * Person
 */
@Entity
@Table(name="person")
@NamedQueries({
  @NamedQuery(name="Person.allPersons", query="SELECT p FROM Person p ORDER BY p.persistenceId ASC"),
  @NamedQuery(name="Person.adultsParmList", query="SELECT p FROM Person p WHERE p.age >= ?1 ORDER BY p.persistenceId ASC"),
  @NamedQuery(name="Person.adultsParmMap", query="SELECT p FROM Person p WHERE p.age >= :age ORDER BY p.persistenceId ASC")
})
public class Person extends PersistentObject {

  private static final long serialVersionUID = 1L;

  @Column(name="age")
  protected int age;
  
  @Column(name="name")
  protected String name;

  public Person() {
    super();
  }

  public Person(final long persistentId) {
    super(persistentId);
  }

  public Person(final long persistentId, final long version, int age, String name) {
    super(persistentId, version);
    this.age = age;
    this.name = name;
  }

  public Person(long persistentId, int age, String name) {
    this(persistentId, 0L, age, name);
  }

  public Person newPersonWithAge(final int newAge) {
    return new Person(persistenceId(), version(), newAge, name);
  }

  public Person newPersonWithName(final String newName) {
    return new Person(persistenceId(), version(), this.age, newName);
  }

  public static Person newPersonFrom(Person person) {
    return new Person(person.persistenceId(), person.version(), person.age, person.name);
  }
}
