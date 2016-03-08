---
layout: post
title: Setup
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 2
version: devel
description: Setup
---

FijiREST runs as part of the Fiji environment. A BentoBox cluster includes FijiREST, meaning nothing else
needs to be downloaded --  simply start the FijiREST server.

To run FijiREST on hardware independent of the HBase cluster, the FijiREST package includes
the necessary Fiji libraries required to access the cluster remotely.

### Setting up FijiREST on a local server

To run FijiREST in a production environment, see [Setup and Run (Production)](#setup-production).

### Set up

If you are using a BentoBox, set the environment variable `${KIJI_REST_HOME}` to point to the `rest` directory
within your BentoBox installation:

    export KIJI_REST_HOME=/path/to/bento/box/rest

If you are not using a BentoBox, download the FijiREST tarball from the Fiji
[Downloads](http://www.fiji.org/getstarted/#Downloads) page, untar the archive, and set `${KIJI_REST_HOME}` to
point to the root directory:

    export KIJI_REST_HOME=/path/to/tarball/fiji-rest-{{site.rest_devel_version}}

Within `${KIJI_REST_HOME}`, you should see the following:

<dl>
<dt>  bin/ </dt>
    <dd>Contains start/stop scripts to control the FijiREST application</dd>
<dt>conf/ </dt>
    <dd>Basic FijiREST and Dropwizard configuration settings</dd>
<dt>docs/ </dt>
    <dd>API docs</dd>
<dt>lib/ </dt>
    <dd>A placeholder directory of additional classpath jars (especially Avro classes)</dd>
<dt>README.md  </dt>
    <dd> A terse version of this document.</dd>
</dl>

(Non-BentoBox users will also see `fiji-rest-{{site.rest_devel_version}}.jar`.)

Installing a Fiji instance (see the [Get Started](http://www.fiji.org/getstarted/) page for details)
is necessary before running FijiREST (FijiREST cannot run on a system without an installed Fiji
instance).  We also recommend that users running the example commands in this tutorial first run
through one of the [Fiji tutorials](http://docs.fiji.org/tutorials.html).

### Startup with Basic Configuration

The FijiREST configuration parameters are located in
`${KIJI_REST_HOME}/conf/configuration.yml`
(`rest/conf/configuration.yml` for BentoBox users). This file is divided into
two major sections:
* The top portion of the file configures FijiREST
* The bottom portion configures [Dropwizard](http://dropwizard.codahale.com/) (including HTTP and logging).

To configure and run FijiREST for your cluster and instance:

1.  Start HBase and Hadoop with a configured Fiji environment. Make sure necessary Avro
classes are accessible either in `$KIJI_CLASSPATH` or in the `${KIJI_REST_HOME}/lib/` directory.

    If you are running a BentoBox, start Hadoop and HBase with `bento start`.

2.  Set the cluster key in `configuration.yml` to the URI of our Fiji
environment, for example `fiji://.env`.

3.  Set the instance key to the list of instances we would like to surface
through the REST service, for example `default`, `prod_instance`,
`dev_instance`:

        cluster: fiji://.env/
        instances:
         - default
         - prod_instance
         - dev_instance

4.  Start FijiREST.

        ~/REST$  ./bin/fiji-rest start

    The default `configuration.yml` file sets up the REST service through port 8080
    and writes all logs to the `${KIJI_REST_HOME}/logs`
    directory. The process will run in the background.

5. Check that FijiREST is running correctly.

        ~/REST$  ./bin/fiji-rest status
        Fiji REST is running. PID is 1234

    (You can also find the process ID in the `${KIJI_REST_HOME}/fiji-rest.pid` file.)

    If FijiREST is not running, consult the logs in `${KIJI_REST_HOME}/logs`.  Often if your FijiREST process
    stops during this stage in the setup, the reason is that FijiREST cannot find the Fiji instances
    described in `${KIJI_REST_HOME}/conf/configuration.yml`.  You will then see something similar to the
    following in `${KIJI_REST_HOME}/logs/console.out`:

            Exception in thread "main" org.fiji.schema.FijiNotInstalledException: Fiji instance
            fiji://localhost:2181/default/ is not installed.

    If so, run `fiji install` and try again.

### Get Instance Status
You can check on the status of Fiji instances using the Dropwizard healthcheck
portal (default port: 8081).

    $ curl http://localhost:8081/healthcheck
    * deadlocks: OK
    * fiji://localhost:2182/default/: OK
    * fiji://localhost:2182/dev_instance/: OK
    * fiji://localhost:2182/prod_instance/: OK

### Logging

There are three types of logs in the logs directory:

    $  ls logs/

<dl>
<dt>app.log</dt>
    <dd>Contains Fiji client logs.</dd>
<dt>requests.log</dt>
    <dd>Contains HTTP logs.</dd>
<dt>console.out</dt>
    <dd>Any other logs which may have been sent to the console.</dd>
</dl>


The logging options (console-logging, log files, syslog, archiving, etc.) are set in the
Dropwizard section of `configuration.yml`. The available options and defaults are detailed
in the [Dropwizard User Manual](http://dropwizard.codahale.com/manual/).

<a id="setup-production"> </a>
## Setup and Run (Production)

Installing FijiREST in a production environment has additional considerations beyond those
described for development systems.

1. Unpack the `fiji-rest` tarball as the `/opt/wibi/fiji-rest directory`:

        $ tar -xzf fiji-rest-{{site.rest_devel_version}}-release.tar.gz -C /opt/wibi/
        $ ln -s fiji-rest-{{site.rest_devel_version}} fiji-rest

    The FijiREST package includes FijiSchema libraries need to be able to access Fiji tables.

1. Add the FijiREST user (with appropriately limited permissions) named "fiji".

        $ sudo useradd fiji

1. Move this script to `/etc/init.d/fiji-rest` and register it as a service with `chkconfig`.

        $ sudo cp /opt/wibi/fiji-rest/bin/fiji-rest.initd  /etc/init.d/fiji-rest
        $ chkconfig --add fiji-rest

To start the service, run:

    $ /sbin/service fiji-rest start

To stop the service, run:

    $ /sbin/service fiji-rest stop
