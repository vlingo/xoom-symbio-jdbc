// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
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
  @NamedQuery(name="Person.allPersons", query="SELECT p FROM Person p ORDER BY p.id ASC"),
  @NamedQuery(name="Person.adultsParmList", query="SELECT p FROM Person p WHERE p.age >= ?1 ORDER BY p.id ASC"),
  @NamedQuery(name="Person.adultsParmMap", query="SELECT p FROM Person p WHERE p.age >= :age ORDER BY p.id ASC")
})
public class Person extends PersistentObject implements ReferenceObject {

  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.SEQUENCE)
  @Column(name = "id", updatable = false, nullable = false)
  protected long id;
  
  @Column(name="age")
  protected int age;
  
  @Column(name="name")
  protected String name;
  
  @Column(name="version")
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
