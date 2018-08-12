# vlingo-symbio-jdbc
Implementation of vlingo-symbio for JDBC.

[![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-jdbc.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-jdbc) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/_latestVersion)

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - General-purpose JDBC: `JDBCTextStateStoreActor`

The `JDBCTextStateStoreActor` has these database delegate implementations:

   - HSQLDB: `HSQLDBStorageDelegate`
   - PostgresSQL: `PostgresStorageDelegate`

Adding additional JDBC storage delegates is a straightforward process requiring a few hours of work.

We welcome you to add support for your favorite database!

### Bintray

```xml
  <repositories>
    <repository>
      <id>jcenter</id>
      <url>https://jcenter.bintray.com/</url>
    </repository>
  </repositories>
  <dependencies>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio</artifactId>
      <version>0.3.7</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-jdbc</artifactId>
      <version>0.3.7</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:0.3.7'
    compile 'io.vlingo:vlingo-symbio-jdbc:0.3.7'
}

repositories {
    jcenter()
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2018 Vaughn Vernon. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
