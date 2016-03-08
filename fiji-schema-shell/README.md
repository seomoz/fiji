

Introduction
============

The FijiSchema DDL Shell implements a _layout definition language_ that
allows you to create, modify, inspect, and delete fiji table layouts. A
_table layout_ is the FijiSchema data dictionary element that defines a set
of column families, qualifiers, etc. and associates these columns with
Avro schemas.

With this shell, you can quickly create tables to use in your FijiSchema
applications. You can add, drop, or modify columns quickly too. These
modifications can be performed without any downtime associated with the
table; the only exception is that modifying a locality group causes
the table to be taken offline while applying changes. But locality group
changes should be rare in running applications.

Finally, this shell allows you to quickly understand the layout of an
existing table. The `DESCRIBE` command will pretty-print the layout of a
table, eliminating the guesswork as to where your coworker has stashed an
important piece of data.


For more information about FijiSchema and the Fiji system, see
[www.fiji.org](http://www.fiji.org).


Running
=======

* Export `$KIJI_HOME` to point to your FijiSchema installation.
  If you source `fiji-env.sh` in the BentoBox, this is taken care of.
* Run `bin/fiji-schema-shell`

This command takes a few options (e.g., to load a script out of a file).
See `bin/fiji-schema-shell --help` for all the available options.

An Example
----------

    CREATE TABLE users WITH DESCRIPTION 'some data'
    ROW KEY FORMAT (username STRING)
    WITH LOCALITY GROUP default WITH DESCRIPTION 'main storage' (
      MAXVERSIONS = INFINITY,
      TTL = FOREVER,
      INMEMORY = false,
      COMPRESSED WITH GZIP,
      FAMILY info WITH DESCRIPTION 'basic information' (
        name "string" WITH DESCRIPTION 'The user\'s name',
        email "string",
        age "int"),
      MAP TYPE FAMILY integers COUNTER WITH DESCRIPTION 'metric tracking data'
    );

    ALTER TABLE users ADD MAP TYPE FAMILY strings { "type" : "string" }
      WITH DESCRIPTION 'ad-hoc string data' TO LOCALITY GROUP default;

    SHOW TABLES;
    DESCRIBE EXTENDED users;
    DROP TABLE users;
    SHOW TABLES;


Language Reference
==================

To read a full reference manual for this component, see 
[The DDL reference in the online FijiSchema user guide](http://docs.fiji.org/userguides/schema/${project.version}/schema-shell-ddl-ref/).

API
===

You can use the `org.fiji.schema.shell.api.Client` object to programmatically
invoke DDL commands from a Java or Scala-based system.

Example:

    import org.fiji.schema.FijiURI;
    import org.fiji.schema.shell.api.Client;

    void exampleMethod() {
      Client client = Client.newInstance(FijiURI.parse("fiji://.env/default"));
      try {
        // One or more calls to executeUpdate(). These will throw DDLException
        // if there's a problem with your DDL statement.
        client.executeUpdate("DROP TABLE t");
      } finally {
        // Always call close when you're done.
        client.close();
      }
    }


You will need to include this artifact in your project's dependency list.
For maven users:

    &lt;dependency&gt;
      &lt;groupId&gt;org.fiji.schema-shell&lt;/groupId&gt;
      &lt;artifactId&gt;fiji-schema-shell&lt;/artifactId&gt;
      &lt;version&gt;${project.version}&lt;/version&gt;
    &lt;/dependency&gt;

