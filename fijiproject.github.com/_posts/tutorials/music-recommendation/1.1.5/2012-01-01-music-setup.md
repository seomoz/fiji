---
layout: post
title: Setup
categories: [tutorials, music-recommendation, 1.1.5]
tags: [music]
order: 2
description: Setup and compile instructions for the music recommendation tutorial.
---
For this tutorial, we assume you are either using the standalone Fiji BentoBox or
have installed the individual components described [here](http://www.fiji.org/getstarted/).
If you don\'t have a working environment yet, you can install the standalone Fiji
BentoBox in [three quick steps!](http://www.fiji.org/#tryit)

### Start a Fiji Cluster

*  If you plan to use a BentoBox, run the following command to set BentoBox-related environment
   variables and start the Bento cluster:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/fiji-env.sh
bento start
{% endhighlight %}
</div>

After BentoBox starts, it displays a list of useful ports for cluster webapps and services.  The
MapReduce JobTracker webapp ([http://localhost:50030](http://localhost:50030) in particular will be
useful for this tutorial.

### Compiling

If you have downloaded the standalone Fiji BentoBox, the code for this tutorial
is already compiled and located in the `${FIJI_HOME}/examples/music/` directory.
Commands in this tutorial will depend on this location:

<div class="userinput">
{% highlight bash %}
export MUSIC_HOME=${FIJI_HOME}/examples/music
{% endhighlight %}
</div>

If you are not using the Fiji BentoBox, set `MUSIC_HOME` to the path of your local
fiji-music repository.

Once you have done this, if you are using Fiji BentoBox you can skip to
"Set your environment variables" if you want to get started playing with the example code.
Otherwise, follow these steps to compile it from source.

The source code for this tutorial can be found in `${MUSIC_HOME}`.
The source is included along with a Maven project. To get started using Maven,
consult [Getting started With Maven]({{site.fiji_url}}/get-started-with-maven) or
the [Apache Maven Homepage](http://maven.apache.org/).

The following tools are required to compile this project:
* Maven 3.x
* Java 6

To compile, run `mvn package` from `${MUSIC_HOME}`. The build
artifacts (.jar files) will be placed in the `${MUSIC_HOME}/target/`
directory. This tutorial assumes you are using the pre-built jars included with
the music recommendation example under `${MUSIC_HOME}/lib/`. If you wish to
use jars of example code that you have built, you should adjust the command
lines in this tutorial to use the jars in `${MUSIC_HOME}/target/`.

### Set your environment variables
After Bento starts, it will display ports you will need to complete this tutorial. It will be useful
to know the address of the MapReduce JobTracker webapp
([http://localhost:50030](http://localhost:50030) by default) while working through this tutorial.

It will be useful to define an environment variable named `FIJI` that holds a Fiji URI to the Fiji
instance we'll use during this tutorial.

<div class="userinput">
{% highlight bash %}
export FIJI=fiji://.env/fiji_music
{% endhighlight %}
</div>

To work through this tutorial, various Fiji tools will require that Avro data
type definitions particular to the working music recommendation example be on the
classpath. You can add your artifacts to the Fiji classpath by running:

<div class="userinput">
{% highlight bash %}
export LIBS_DIR=${MUSIC_HOME}/lib
export FIJI_CLASSPATH="${LIBS_DIR}/*"
{% endhighlight %}
</div>

### Install Fiji and Create Tables

Install your Fiji instance:

<div class="userinput">
{% highlight bash %}
fiji install --fiji=${FIJI}
{% endhighlight %}
</div>

Create the Fiji music tables:

<div class="userinput">
{% highlight bash %}
fiji-schema-shell --fiji=${FIJI} --file=${MUSIC_HOME}/music_schema.ddl
{% endhighlight %}
</div>

This command uses [fiji-schema-shell](https://github.com/fijiproject/fiji-schema-shell)
to create the tables using the FijiSchema DDL, which makes specifying table layouts easy.
See [the FijiSchema DDL Shell reference]({{site.userguide_schema_1_4_0}}/schema-shell-ddl-ref)
for more information on the FijiSchema DDL.

##### (Optional) Generate Data

The music recommendation example comes with pregenerated song data in
`${MUSIC_HOME}/example_data`.  These .json files contain randomly-generated song information
and randomly-generated usage information for this tutorial.

If you wish to generate new data, wipe the data directory, then use the python script provided.

<div class="userinput">
{% highlight bash %}
rm ${MUSIC_HOME}/example_data/*
${MUSIC_HOME}/bin/data_generator.py --output-dir=${MUSIC_HOME}/example_data/
{% endhighlight %}
</div>

This should generate 3 JSON files: `example_data/song-dist.json`, `example_data/song-metadata.json`
and `example_data/song-plays.json`.

### Upload Data to HDFS

Upload the data set to HDFS (this step is required, even if you did not generate new data):

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir fiji-mr-tutorial
hadoop fs -copyFromLocal ${MUSIC_HOME}/example_data/*.json fiji-mr-tutorial/
{% endhighlight %}
</div>

Upload the import descriptor for the bulk-importer in the next section to HDFS:

<div class="userinput">
{% highlight bash %}
hadoop fs -copyFromLocal ${MUSIC_HOME}/import/song-plays-import-descriptor.json fiji-mr-tutorial/
{% endhighlight %}
</div>
