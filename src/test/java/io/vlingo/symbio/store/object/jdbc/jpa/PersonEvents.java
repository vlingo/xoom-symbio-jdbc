// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.symbio.Source;
/**
 * PersonEvents
 */
public final class PersonEvents {
  
  public static class PersonAdded extends Source<PersonAdded> {
    public final String personId;
    public final int age;
    public final String name;
    public final int version;
    
    public PersonAdded(final Person person) {
      this(String.valueOf(person.id), person.age, person.name, person.version);
    }
    
    public PersonAdded(final String personId, final int age, final String name, final int version) {
      super();
      this.personId = personId;
      this.age = age;
      this.name = name;
      this.version = version;
    }
  }

  public static class PersonRenamed extends Source<PersonRenamed> {
    
    public final String personId;
    public final String name;
    
    public PersonRenamed(final Person person) {
      this(String.valueOf(person.id), person.name);
    }
    
    public PersonRenamed(final String personId, final String name) {
      super();
      this.personId = personId;
      this.name = name;
    }
  }
}
