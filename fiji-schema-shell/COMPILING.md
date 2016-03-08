
Compiling
=========

    mvn install

This depends on FijiSchema. If you depend on local changes to FijiSchema, you
will need to run `mvn install` in that project first.

Running
=======

* Export `$KIJI_HOME` to point to your FijiSchema installation.
* Run `bin/fiji-schema-shell`

This command takes a few options (e.g., to load a script out of a file).
See `bin/fiji-schema-shell --help` for all the available options.
