FijiSchema
==========

FijiSchema is a layout and schema management layer on top of Apache
HBase. FijiSchema supports complex, compound data types using Avro
serialization, as well as cell-level evolving schemas that dynamically
encode version information.

FijiSchema includes both commandline tools and a simple Java API to
manipulate tables, layouts, and data. It also provide native MapReduce
Input/Output formats for Fiji tables.

For more information about FijiSchema, see
[the Fiji project homepage](http://www.fiji.org).

Compilation
-----------

FijiSchema requires [Apache Maven 3](http://maven.apache.org/download.html) to build. It
may be built with the command

    mvn clean package

or by executing `bin/fiji` from the root of a checkout. This will create a release in the
target directory.

Installation
------------

FijiSchema requires an HBase and Hadoop cluster to run. Confirm that `$HADOOP_HOME` and
`$HBASE_HOME` are set appropriately. Then issue the following command from your FijiSchema
root directory:

    bin/fiji install

This will install a Fiji instance on the hbase cluster named 'default'.

Usage
-----

For commandline tool usage, issue the command:

    bin/fiji

Further documentation is available at the Fiji project
[Documentation Portal](http://docs.fiji.org)

Known CDH5 Issues
-----------------

Below are some known issues with the CDH5 port, incompatibilities, etc. Remove
this section when this is done.

* Code depends on HConnectionManager.createConnection() which doesn't exist
  HBase < 0.94.11
* Needs an implementation of table pool. Code is commented out.
* Security is disabled. Code is commented out.
