# vlingo-symbio-jdbc

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-jdbc.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-jdbc) [![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-jdbc.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-jdbc) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/_latestVersion) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The vlingo/PLATFORM implementation of vlingo/symbio for JDBC.

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - General-purpose JDBC: `JDBCStateStoreActor`

The `JDBCTextStateStoreActor` has these database delegate implementations:

   - HSQLDB: `HSQLDBStorageDelegate`
   - PostgresSQL: `PostgresStorageDelegate`

Adding additional JDBC storage delegates is a straightforward process requiring a few hours of work.

We welcome you to add support for your favorite database!

## Docker and Bouncing the Server Volume
Postrgres must be run for some tests. See the `pgbounce.sh`. This shell script can be used to bounce the Postgres volume named in `docker-compose.yml`:

  `vlingo-symbio-jdbc-postgres`

Run the server using the following, which both stops the current instance and then starts a new instance.

`$ ./pgbounce.sh`


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
      <version>1.2.9</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-jdbc</artifactId>
      <version>1.2.9</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:1.2.9'
    compile 'io.vlingo:vlingo-symbio-jdbc:1.2.9'
}

repositories {
    jcenter()
}
```

License (See LICENSE file for full license)
-------------------------------------------
Copyright © 2012-2020 VLINGO LABS. All rights reserved.

This Source Code Form is subject to the terms of the
Mozilla Public License, v. 2.0. If a copy of the MPL
was not distributed with this file, You can obtain
one at https://mozilla.org/MPL/2.0/.
