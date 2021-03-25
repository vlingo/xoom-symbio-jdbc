# vlingo-symbio-jdbc

[![Javadocs](http://javadoc.io/badge/io.vlingo/vlingo-symbio-jdbc.svg?color=brightgreen)](http://javadoc.io/doc/io.vlingo/vlingo-symbio-jdbc) [![Build](https://github.com/vlingo/vlingo-symbio-jdbc/workflows/Build/badge.svg)](https://github.com/vlingo/vlingo-symbio-jdbc/actions?query=workflow%3ABuild) [ ![Download](https://api.bintray.com/packages/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/images/download.svg) ](https://bintray.com/vlingo/vlingo-platform-java/vlingo-symbio-jdbc/_latestVersion) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/vlingo-platform-java/symbio)

The VLINGO XOOM SYMBIO implementation for JDBC for Reactive storage using Event-Sourcing, Key-Value, and Object storage.

Docs: https://docs.vlingo.io/vlingo-symbio

### Important
If using snapshot builds [follow these instructions](https://github.com/vlingo/vlingo-platform#snapshots-repository) or you will experience failures.

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
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
    <dependency>
      <groupId>io.vlingo</groupId>
      <artifactId>vlingo-symbio-jdbc</artifactId>
      <version>1.5.2</version>
      <scope>compile</scope>
    </dependency>
  </dependencies>
```

```gradle
dependencies {
    compile 'io.vlingo:vlingo-symbio:1.5.2'
    compile 'io.vlingo:vlingo-symbio-jdbc:1.5.2'
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
