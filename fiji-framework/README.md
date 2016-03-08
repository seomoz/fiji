fiji-framework
==============

This repository contains a definition for the entire Fiji framework.

Each module (FijiSchema, FijiMR, etc) can be depended upon individually by users. But
if you are depending on many such modules, it can be difficult to ensure that you
have selected compatible versions of each. This repository includes two pom files
that you can use to depend on the most recent consistent release of all Fiji
components.


Including The Entire Fiji Framework
-----------------------------------

The easiest way to get started is to declare a direct dependency on the entire Fiji
framework.  This will include all Fiji client artifacts as part of your project's
dependencies.  This may be more artifacts than you require, but is the simplest way
to get started.

To depend on the entire framework:

    <dependencies>
      ...

      <dependency>
        <groupId>org.fiji.framework</groupId>
        <artifactId>fiji-framework</artifactId>
        <version>1.1.2</version>
      </dependency>

      ...
    </dependencies>


This will automatically depend on the `fiji-cdh4-platform`. You can choose a
different platform if you'd like by explicitly depending on one.

You will also need to tell Maven where to find Fiji components. You should add
a `<repositories>` block like so:

    <repositories>
      <repository>
        <id>fiji-repos</id>
        <name>fiji-repos</name>
        <url>https://repo.wibidata.com/artifactory/fiji</url>
      </repository>
    </repositories>

Note that if you need to depend on any Fiji test resources, you should
also depend on the fiji-framework-test artifact.

For example:

    <dependencies>
      ...

      <dependency>
        <groupId>org.fiji.framework</groupId>
        <artifactId>fiji-framework-test</artifactId>
        <version>1.1.2</version>
        <scope>test</scope>
      </dependency>

      ...
    </dependencies>

If you want to pick and choose The `framework-pom` parent also allows you to
pick and choose which Fiji modules you depend on (for example, FijiSchema but
not FijiMR), you can do so by using `framework-pom` as your parent pom. This
also lets you include Fiji modules without explicitly specifying the version
and scope. See the next section for more details.


Selecting Individual Components
-------------------------------

If you want to have control over which individual components are included in your
project, you should declare the following as the parent pom:

    <parent>
      <groupId>org.fiji.framework</groupId>
      <artifactId>framework-pom</artifactId>
      <version>1.1.2</version>
    </parent>


This includes a `<dependencyManagement>` section that describes the version numbers of
compatible Fiji framework components. Your pom file's `<dependencies>` section can then
include just the `groupId` and `artifactId` components for each dependency; the version
will be populated from the parent pom.

For example:

    <dependency>
      <groupId>org.fiji.schema</groupId>
      <artifactId>fiji-schema</artifactId>
    </dependency>

You will still need to specify which components you rely on. e.g., FijiSchema, FijiMR,
etc. You must also select a `fiji-platform` definition to specify which versions of
Hadoop and HBase you want to use.

For example:

    <dependency>
      <groupId>org.fiji.platforms</groupId>
      <artifactId>fiji-cdh4-platform</artifactId>
    </dependency>

