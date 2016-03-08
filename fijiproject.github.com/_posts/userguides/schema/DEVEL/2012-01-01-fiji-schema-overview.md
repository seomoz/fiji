---
layout: post
title: What is FijiSchema?
categories: [userguides, schema, devel]
tags : [schema-ug]
version: devel
order : 1
description: Overview.
---

## What is FijiSchema?

FijiSchema provides a simple Java API and command line interface for
importing, managing, and retrieving data from HBase.


## Key Features

- Set up HBase layouts using user-friendly tools including a DDL
- Implement HBase best practices in table management
- Use evolving Avro schema management to serialize complex data
- Perform both short-request and batch processing on data in HBase
- Import data from HDFS into structured HBase tables

FijiSchema promotes the use of entity-centric data modeling, where
all information about a given entity, including both dimensional and
transaction data, is encoded within the same row. This approach is
particularly valuable for user-based analytics such as targeting,
recommendations, and personalization.

## Using FijiSchema in Your Project

You will need to include FijiSchema as a dependency in your project.
If you're using Maven, this can be included as follows in your 
pom.xml in the `<dependencies>` block:

    <dependency>
      <groupId>org.fiji.schema</groupId>
      <artifactId>fiji-schema</artifactId>
      <version>{{site.schema_devel_version}}</version>
      <scope>provided</scope>
    </dependency>

    <dependency>
      <groupId>org.fiji.platforms</groupId>
      <artifactId>fiji-cdh4-platform</artifactId>
      <version>1.1.0</version>
      <scope>provided</scope>
    </dependency>

Because you typically launch Fiji applications by running `bin/fiji jar
/path/to/example.jar`, you can list this as a `provided` dependency; the runtime
environment will provide the implementation jars, so you do not need to bundle them in
your application. If you intend to bundle your application (e.g., for deployment within a
Tomcat container), you should list your scope as:

    <scope>compile</scope>

This ensures that Fiji's jars will be included in your assembled application.

You will need to compile against a specific version of Hadoop and HBase.
The `fiji-cdh4-platform` dependency specifies which Hadoop platform you depend on; in this
case, CDH4. See [the fiji-platforms repository](https://github.com/fijiproject/fiji-platforms)
for more information.

You'll probably need to configure your Maven `settings.xml` to locate these dependencies.
See [Getting started with Maven]({{ site.fiji_url }}/get-started-with-maven)
for more details.

