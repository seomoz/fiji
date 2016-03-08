FijiREST ${project.version}
===========================

FijiREST is a RESTfulll interface for interacting with FijiSchema.

For more information about FijiREST, see
[the FijiREST user guide](http://docs.fiji.org/userguides.html).

For more information about FijiSchema, see
[the Fiji project homepage](http://www.fiji.org).

Further documentation is available at the Fiji project
[Documentation Portal](http://docs.fiji.org)

Installing FijiREST (requires root privileges)
--------------------------------

* It's assumed that where FijiREST is unpacked is called $KIJI\_REST\_HOME By default, this is
assumed to be /opt/wibi/fiji-rest. This can be changed by modifying the KIJI\_REST\_HOME variable
in bin/fiji-rest.initd script.
* Create a non-privileged user called "fiji" which will be used to run the service. This can be
changed by modifying the KIJI\_REST\_USER variable in bin/fiji-rest.initd script.
  * sudo useradd fiji
* Copy $KIJI\_REST\_HOME/bin/fiji-rest.initd as /etc/init.d/fiji-rest
  * sudo cp $KIJI\_REST\_HOME/bin/fiji-rest.initd /etc/init.d/fiji-rest
* chkconfig --add fiji-rest

Starting a local FijiREST server
--------------------------------

Any relevant Avro classes that are necessary for interaction of FijiREST with the underlying Fiji
tables must be included on the classpath upon instantiation
of the server. This is done by placing the jar containing the necessary Avro classes in the
$KIJI\_REST\_HOME/lib folder.

$ cd $KIJI\_REST\_HOME

$ ./bin/fiji-rest start

This will launch the service in the background with the pid of the process located in
$KIJI\_REST\_HOME/fiji-rest.pid. The application and request logs can be found
under $KIJI\_REST\_HOME/logs.

### Alternatively as root:
$ /sbin/service fiji-rest start

Stopping a local FijiREST server
--------------------------------

### If run as non-root:
$ cat $KIJI\_REST\_HOME/fiji-rest.pid | xargs kill

### As root:
$ /sbin/service fiji-rest stop

Setting up configuration.yml
----------------------------

The configuration.yml file (located in $KIJI\_REST\_HOME/conf/configuration.yml) is a YAML file used
to configure the FijiREST server. The following key is required:

- "cluster" is the base cluster's fiji URI.

The following is an example of the contents of a proper configuration.yml file:

"cluster" : "fiji://localhost:2181/" #The base cluster URI

- "instances" is an array of instances that will be made visible to users of the REST service.

If there is no instance array, or it has nothing in it, all instances will be made available.

There is optional key to turn on global cross-origin resource sharing (CORS).

"cors" : "true" #If not set, defaults to false

"cacheTimeout" sets the timeout in minutes before clearing the cache of instances and tables

"remote-shutdown" #when set to true, the admin task to shutdown the fiji-rest via REST command is enabled

FijiREST is implemented using DropWizard. See
[Dropwizard's User Manual](http://dropwizard.codahale.com/manual/core/#configuration-defaults)
for additional Dropwizard-specific configuration options such as server settings
and logging options (console-logging, log files, and syslog).

Creating a FijiREST Plugin
--------------------------

To create a FijiREST plugin project from the maven archetype, use the following command:

mvn archetype:generate \
-DarchetypeCatalog=https://repo.wibidata.com/artifactory/fiji-packages/archetype-catalog.xml

From there you can choose the org.fiji.rest:plugin-archetype (A skeleton FijiREST plugin project.)
option.

Disabling the "standard" FijiREST plugin
---------------------------

The standard FijiREST plugin is what includes the "/v1" endpoint. Some users may
want to disable this access if FijiREST is accessible to a wider audience. For
example, if someone deploys a custom plugin to access a certain sub-section of
Fiji tables, then disabling access to the "/v1" endpoint will prevent users from
reading data from all other tables/restrict access to FijiREST. This also keeps
the deployment of plugins consistent by treating the "standard" plugin as
separate rather than built-in to the application.

To disable the "standard" FijiREST plugin, simply remove the
standard-plugin-<version>.jar from the lib/ directory of FijiREST and restart!

Issues and mailing lists
------------------------

Users are encouraged to join the Fiji mailing lists: user@fiji.org and dev@fiji.org (for developers).

Please report your issues at [the FijiREST JIRA project](https://jira.fiji.org/browse/REST).
