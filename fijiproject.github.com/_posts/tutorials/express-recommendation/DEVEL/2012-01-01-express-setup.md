---
layout: post
title: Setting up Fiji and HDFS
categories: [tutorials, express-recommendation, devel]
tags: [express-music]
order: 3
description: Setup for FijiExpress Tutorial
---

### Install Fiji BentoBox

If you don't have a working environment yet, install the standalone Fiji BentoBox in [three quick
steps!](http://www.fiji.org/#tryit)

### Start a Fiji Cluster

If you plan to use a BentoBox, run the following command to set BentoBox-related environment
variables and start the Bento cluster.  To start a BentoBox with HBase:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/fiji-env.sh
bento start
{% endhighlight %}
</div>

To start a BentoBox with Cassandra:

<div class="userinput">
{% highlight bash %}
cd <path/to/bento>
source bin/fiji-env.sh
cassandra-bento start
{% endhighlight %}
</div>

After BentoBox starts, it displays a list of useful ports for cluster webapps and services.  The
MapReduce JobTracker webapp ([http://localhost:50030](http://localhost:50030) in particular will be
useful for this tutorial.


-  If you are running Fiji without a BentoBox, there are a few things you'll need to do to make sure
   your environment behaves the same way as a BentoBox:


<div id="accordion-container">
  <h2 class="accordion-header">Starting Fiji in Non-BentoBox Systems </h2>
  <div class="accordion-content">
<ol>
<li>Make sure HDFS is installed and started.</li>
<li>Make sure MapReduce is installed, that <code>HADOOP_HOME</code> is set to your
	<code>MR</code> distribution, and that MapReduce is started.</li>
<li>If you are planning to use HBase with Fiji, make sure HBase is installed, that
  <code>HBASE_HOME</code> is set to your <code>hbase</code> distribution, and that HBase is
  started.</li>
<li>Export <code>KIJI_HOME</code> to the root of your <code>fiji</code> distribution.</li>
<li>Export <code>PATH=${PATH}:${KIJI_HOME}/bin</code>.</li>
<li>Export <code>EXPRESS_HOME</code> to the root of your <code>fiji-express</code> distribution.</li>
<li>Export <code>PATH=${PATH}:${EXPRESS_HOME}/bin</code></li>
</ol>
  </div>
</div>

When the tutorial refers to the BentoBox, you'll know that you'll have to manage your
Fiji cluster appropriately.

### Set Tutorial-Specific Environment Variables

Define an environment variable named `KIJI` that holds a Fiji URI to the Fiji
instance we'll use during this tutorial.  For an HBase-backed Fiji instance:

<div class="userinput">
{% highlight bash %}
export KIJI=fiji://.env/fiji_express_music
{% endhighlight %}
</div>

And for a Cassandra-backed Fiji instance:

<div class="userinput">
{% highlight bash %}
export KIJI=fiji-cassandra://localhost:2181/localhost:9042/fiji_express_music
{% endhighlight %}
</div>

The code for this tutorial is located in the `${KIJI_HOME}/examples/express-music/` directory.
Commands in this tutorial will depend on this location.

Next, set a variable for the tutorial location:

<div class="userinput">
{% highlight bash %}
export MUSIC_EXPRESS_HOME=${KIJI_HOME}/examples/express-music
{% endhighlight %}
</div>

### Install Fiji

Install your Fiji instance:

<div class="userinput">
{% highlight bash %}
fiji install --fiji=${KIJI}
{% endhighlight %}
</div>

### Create Tables

The file `music-schema.ddl` defines table layouts that are used in this tutorial:
<div id="accordion-container">
  <h2 class="accordion-header"> music-schema.ddl </h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/fijiproject/fiji-express-music/raw/{{site.music_express_devel_branch}}/src/main/resources/org/fiji/express/music/music-schema.ddl"> </script>
  </div>
</div>

Create the Fiji music tables that have layouts described in `music-schema.ddl`.

<div class="userinput">
{% highlight bash %}
${KIJI_HOME}/schema-shell/bin/fiji-schema-shell --fiji=${KIJI} --file=${MUSIC_EXPRESS_HOME}/music-schema.ddl
{% endhighlight %}
</div>

This command uses fiji-schema-shell
to create the tables using the FijiSchema DDL, which makes specifying table layouts easy.
See [the FijiSchema DDL Shell reference]({{site.userguide_schema_devel}}/schema-shell-ddl-ref)
for more information on the FijiSchema DDL.

Verify the Fiji music tables were correctly created:

<div class="userinput">
{% highlight bash %}
fiji ls ${KIJI}
{% endhighlight %}
</div>

You should see the newly-created `songs` and `users` tables.  If you are using an HBase-backed Fiji
instance:

    fiji://localhost:2181/express_music/songs
    fiji://localhost:2181/express_music/users

And for a Cassandra-backed Fiji instance:

    fiji-cassandra://localhost:2181/localhost:9042/express_music/songs
    fiji-cassandra://localhost:2181/localhost:9042/express_music/users

### Upload Data to HDFS

HDFS stands for Hadoop Distributed File System.  If you are running the BentoBox,
it is running as a filesystem on your machine atop your native filesystem.
This tutorial demonstrates loading data from HDFS into Fiji tables, which is a typical
first step when creating FijiExpress applications.

*  Upload the data set to HDFS:

<div class="userinput">
{% highlight bash %}
hadoop fs -mkdir express-tutorial
hadoop fs -copyFromLocal ${MUSIC_EXPRESS_HOME}/example_data/*.json express-tutorial/
{% endhighlight %}
</div>

You're now ready for the next step, [Importing Data](../express-importing-data).

### Fiji Administration Quick Reference

Here are some of the Fiji commands introduced on this page and a few more useful ones:

**Start a BentoBox Cluster**

To start an HBase-backed BentoBox:

{% highlight bash %}
cd <path/to/bento>
source bin/fiji-env.sh
bento start
{% endhighlight %}

To start a Cassandra-backed BentoBox:

{% highlight bash %}
cd <path/to/bento>
source bin/fiji-env.sh
cassandra-bento start
{% endhighlight %}

**Stop your BentoBox Cluster**

If using an HBase-backed BentoBox:

{% highlight bash %}
bento stop
{% endhighlight %}

For a Cassandra-backed BentoBox:

{% highlight bash %}
cassandra-bento stop
{% endhighlight %}

**Default location of the MapReduce JobTracker web app**:
[http://localhost:50030](http://localhost:50030)

**Install a Fiji instance**:

{% highlight bash %}
fiji install --fiji=<URI/of/instance>
{% endhighlight %}

URIs for HBase-backed Fiji instances use the scheme `fiji-hbase` and take the form:

{% highlight bash %}
fiji-hbase://<zookeeper host>:<zookeeper port>/<instance name>
{% endhighlight %}

URIs for Cassandra-backed Fiji instances use the scheme `fiji-cassandra` and take the form:

{% highlight bash %}
fiji-cassandra://<zookeeper host>:<zookeeper port>/<cassandra host>:<cassandra native transport port>/<instance name>
{% endhighlight %}

URIs with the default scheme, `fiji`, use HBase as well:

{% highlight bash %}
fiji://<zookeeper host>:<zookeeper port>/<instance name>
{% endhighlight %}

For more information about Fiji URIs, see the API documentation for `FijiURI`.

**Running compiled FijiExpress jobs**

To run a FijiExpress job, you invoke a command of the following form:

{% highlight bash %}
express.py job \
    --jars=<list of JAR files, separated by colon> \
    --class=org.MyFijiApp.MyJob \
    [--mode=local|hdfs] \
    [job-specific options]
{% endhighlight %}

The `mode=hdfs` flag indicates that FijiExpress should run the job against the Hadoop cluster
versus in Cascading's local environment.  The `--jars` flag indicates JAR files needed
to run the command.

**Launching the FijiExpress shell**

FijiExpress includes an interactive shell that can be used to execute FijiExpress flows. To launch
the shell, you invoke a command of the following form:

{% highlight bash %}
express.py shell \
    --jars=<list of JAR files, separated by colon> \
    [--mode=local|hdfs]
{% endhighlight %}

If the mode flag is set to 'hdfs', `mode=hdfs`, the shell will run jobs using Scalding's Hadoop
mode. For normal usage against a hadoop cluster, this option should be used.

If the `--jars` flag is nonempty, the jar files specified will be placed on the classpath.  You
typically use this flag to point to a JAR containing your Express jobs, as well as JARs with
external libraries or compiled avro classes.

To execute multi-line statements in the shell, use paste mode. This can be used to execute existing
FijiExpress code. To start paste mode, enter the following command into a running FijiExpress shell:

    :set paste

Type `Ctrl+D` to end paste mode and execute the entered code.
