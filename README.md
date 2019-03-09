# vlingo-symbio-jdbc
Implementation of vlingo-symbio for JDBC.

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-jdbc.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-jdbc) [![Build Status](https://travis-ci.org/vlingo/vlingo-symbio-jdbc.svg?branch=master)](https://travis-ci.org/vlingo/vlingo-symbio-jdbc) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/_latestVersion) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

### State Storage
The `StateStore` is a simple object storage mechanism that can be run against a number of persistence engines.
Available JDBC storage implementations:

   - General-purpose JDBC: `JDBCTextStateStoreActor`

The `JDBCTextStateStoreActor` has these database delegate implementations:

   - HSQLDB: `HSQLDBStorageDelegate`
   - PostgresSQL: `PostgresStorageDelegate`

Adding additional JDBC storage delegates is a straightforward process requiring a few hours of work.

We welcome you to add support for your favorite database!

### Running Tests

Postrgres must be run for some tests. We provide a Docker image for Postgres, which can be run from this project's root using the following command line:

`$ docker-compose up`

You can also use the `-d` option to run it in the background:

`$ docker-compose up -d`

However, this may not work well for some test cases because you may need to start and stop Docker Postgres frequently during some development on this repository project code. Why? Currently Postrgres data may not be cleaned up well by tests, so the next test run will encounter some unique constraint violations (same PKs reused, etc.). Sure, we'd love to improve the tests, such as with random key generation, which you could contribute :)

Until the tests are improved, before each test run execute this in a separate terminal window:

`$ . ./src/test/postgres/pgclean.sh`

This is what it does:

```# start postgres in docker with a clean data for test
docker-compose down
docker volume rm $(docker volume ls -q)
docker-compose up
```

The Docker image is run in the foreground because after each test session you can go to the terminal window and `^C` out of the process. Then just re-run the same script to get a clean Postgres test environment. If you'd like to help us make our test environment better by incorporating better tools and procedures, we welcome your contribution.

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
      <version>0.8.2</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-jdbc</artifactId>
      <version>0.8.2</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:0.8.2'
    compile 'io.vlingo:vlingo-symbio-jdbc:0.8.2'
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
