---
layout: post
title: What is FijiMR?
categories: [userguides, mapreduce, 1.2.9]
tags : [mapreduce-ug]
version: 1.2.9
order : 1
description: Overview.
---

FijiMR allows FijiSchema users to employ MapReduce-based techniques to develop many kinds of
applications, including those using machine learning and other complex analytics.

FijiMR is organized around three core MapReduce job types: _Bulk Importers_, _Producers_ and
_Gatherers_.

 * _Bulk Importers_ make it easy to efficiently load data into Fiji tables from a variety of
   formats, such as JSON or CSV files stored in HDFS.
 * _Producers_ are entity-centric operations that use an entity's existing data to generate new
   information and store it back in the entity's row. One typical use-case for producers is to
   generate new recommendations for a user based on the user's history.
 * _Gatherers_ provide flexible MapReduce computations that scan over Fiji table rows and output
   key-value pairs. By using different outputs and reducers, gatherers can export data in a variety
   of formats (such as text or Avro) or into other Fiji tables.

Finally, FijiMR allows any of these jobs to combine the data they operate on with external
_KeyValueStores_. This allows the user to join data sets stored in HDFS and Fiji.

Unlike FijiSchema, where the classes most relevant to application developers were usually concrete,
these core job types exist in FijiMR as abstract classes (such as
[`FijiProducer`]({{site.api_mr_1_2_9}}/produce/FijiProducer.html)). It is typically up to the
application developer to subclass the appropriate class in their application and implement their
application's analysis logic in a few methods (such as
[`FijiProducer`]({{site.api_mr_1_2_9}}/produce/FijiProducer.html)'s `produce()` and
`getDataRequest()`). They can then point the job at the appropriate Fiji table using either the
`fiji` command line tools or programmatically using one of the framework's JobBuilders (such as
[`FijiProduceJobBuilder`]({{site.api_mr_1_2_9}}/produce/FijiProduceJobBuilder.html)) that make
launching these jobs easy.  Fiji can also record metadata about jobs run using FijiMR to provide
a historical view.

The FijiMR Library provides a growing repository of implemented solutions to many common use-cases.
These solutions can be used directly or as example code. Both FijiMR and the FijiMR Library are
included with distributions of Fiji BentoBox.

In the sections of this guide that follow, the core job types will be explained in greater detail.
Motiviation, example code snippets, and (where appropriate) a description of reference
implementations in the FijiMR Library will be given for each. This guide also contains an in-depth
description of how to use  _KeyValueStores_ to expose side-data stored in HDFS and Fiji through a
consistent interface to your MapReduce jobs. Finally, this guide contains a description of the
command line tools included with FijiMR and facilities that make it easier to test FijiMR
application code.

## Using FijiMR in Your Project

You will need to include FijiMR as a dependency in your project. If you're
using Maven, this can be included as follows:

    <dependency>
      <groupId>org.fiji.mapreduce</groupId>
      <artifactId>fiji-mapreduce</artifactId>
      <version>1.2.9</version>
      <scope>provided</scope>
    </dependency>

If you want to use bulk importers or other classes in the FijiMR Library, you will
need to include:

    <dependency>
      <groupId>org.fiji.mapreduce.lib</groupId>
      <artifactId>fiji-mapreduce-lib</artifactId>
      <version>1.1.8</version>
      <scope>provided</scope>
    </dependency>

Different versions of Hadoop are incompatible with one another at a binary level. The version
of FijiMR specified above can be used with Hadoop 2-based systems (e.g., CDH 4). You can use
FijiMR with a Hadoop 1-based system by declaring a dependency on a different build of FijiMR
without changing your source code. Just use the following dependency instead:

    <dependency>
      <groupId>org.fiji.mapreduce</groupId>
      <artifactId>fiji-mapreduce</artifactId>
      <version>1.2.9</version>
      <classifier>hadoop1</classifier>
      <scope>provided</scope>
    </dependency>

You can also explicitly specify `<classifier>hadoop2</classifier>` if you'd like, although
this is the default. If you use the `bin/fiji` command to launch your Fiji application,
it will automatically detect which version of Hadoop is present and use the appropriate
build of FijiMR at runtime without any changes to your code.

* You will also need a dependency on FijiSchema. See [the FijiSchema
  documentation]({{site.userguide_schema_1_5_0}}/fiji-schema-overview/) for this information.
* You'll probably need to configure your Maven `settings.xml` to locate these dependencies.
  See [Getting started with Maven](http://www.fiji.org/get-started-with-maven)
  for more details.
