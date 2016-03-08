(c) Copyright 2013 WibiData, Inc.

Fiji BentoBox ${project.version}
================================

The Fiji BentoBox is an SDK for developing Big Data Applications with the
Fiji framework. It includes a complete set of Fiji framework modules,
with compatible versions of each.

This fiji-bento release (${project.version}) includes:

* `fiji-schema` ${fiji-schema.version}: Included at the top-level of the
  distribution, fiji-schema provides a simple Java API and command-line tools
  for storing and managing typed data in HBase.
* `fiji-mapreduce` ${fiji-mapreduce.version}: Included in the `lib` directory,
  fiji-mapreduce provides a simple Java API and command-line tools for using
  Hadoop MapReduce to import and process data in Fiji tables.
* `fiji-mapreduce-lib` ${fiji-mapreduce-lib.version}: Included in the `lib`
  directory, fiji-mapreduce-lib is a Java library of utilities for writing
  MapReduce jobs on Fiji tables, as well as ready to use producers, gatherers,
  and importers.
* `bento-cluster` ${bento-cluster.version}: Located in the `cluster`
  directory, bento-cluster allows users to run HDFS, MapReduce, and HBase
  clusters easily on the local machine.
* `fiji-schema-shell` ${fiji-schema-shell.version}: Included in the
  `schema-shell` directory, fiji-schema-shell provides a layout definition
  language for use with `fiji-schema`.
* `fiji-hive-adapter` ${fiji-hive-adapter.version}: Included in the
  `hive-adapter` directory, fiji-hive-adapter provides a SerDe for
  Hive to use Fiji tables as external Hive tables.
* `fiji-express` ${fiji-express.version}: Included in the `express`
  directory, fiji-express provides a Scala DSL for analyzing and modeling
  data stored in Fiji.
* `fiji-express-tools` ${fiji-express-tools.version}: included in the `express`
  directory, fiji-express-tools provides a REPL and related tools for Express.
* `fiji-modeling` ${fiji-modeling.version}: Included in the `modeling`
  directory, fiji-modeling provides a formalization for training, applying,
  and evaluation machine learning models built on top of fiji-express.
* `fiji-scoring` ${fiji-scoring.version}: Included in the `scoring` directory,
  fiji-scoring is a library and server that supports the real-time per-row
  calculations on fiji tables.
* `fiji-model-repository` {fiji-model-repository.version}: Included in the
  `model-repo` directory, the fiji-model-repository is a library which permits
  storage of trained fiji-modeling in a maven repository, indexed by a fiji
  table. fiji-scoring can use this models stored in this repository to for its
  scoring.
* `fiji-phonebook` ${fiji-phonebook.version}: Included in the
  `examples/phonebook` directory, fiji-phonebook is an example standalone
  application (with source code) that stores, processes, and retrieves data
  from a Fiji instance.
* `fiji-music` ${fiji-music.version}: Included in the
  `examples/music` directory, fiji-music is an example of loading the listening
  history of users of a music service into a Fiji table, and then generating new
  music recommendations.
* `fiji-express-music` ${fiji-express-music.version}: Included in the
  `examples/express-music` directory, fiji-express-music is a fiji-express
  implementation of the fiji-music example.
* `fiji-express-examples` ${fiji-express-examples.version}: Included in the
  `examples/express-examples` directory, fiji-express-examples provides example
  usage of fiji-express processing a newsgroups dataset.
* API documentation is made available in the `docs` directory.

Installation
------------

### Untar fiji-bento
Untar your fiji-bento distribution into a location convenient for
you. We suggest your home directory. In the future we'll call the
path to your fiji-bento distribution `$KIJI_HOME`.

### Configure your system to use fiji-bento.
fiji-bento includes a script that will configure your
environment to use the HDFS, MapReduce and HBase clusters managed by
the included bento-cluster distribution, as well as fiji and
fiji-schema-shell. To configure your environment, run:

> `source $KIJI_HOME/bin/fiji-env.sh`

Add this line to your `~/.bashrc` file to ensure your environment is
always configured for fiji-bento.

`fiji-env.sh` configures your environment to use the Hadoop
ecosystem distributions included with bento-cluster, as well as the
clusters we'll soon use bento-cluster to start. It also adds the
`hadoop`, `hbase`, `bento`, `fiji` and `fiji-schema-shell` scripts to
your `$PATH`, making them available as top-level commands.

### Start bento-cluster.
Now that your system is configured, you can use the `bento` script to
start your local HDFS, MapReduce, and HBase clusters.

> `bento start`

### Use fiji
With the clusters started, you can run Hadoop ecosystem tools like
fiji. For example, to list your home directory in HDFS, run:

> `hadoop fs -ls`

Or, to install the default fiji instance in HBase, run:

> `fiji install`

The `hadoop`, `hbase`, `fiji` and `fiji-schema-shell` scripts are
available for use. You can also use other Hadoop ecosystem tools
like hive or pig and they will use the local clusters managed by
bento-cluster when run in an environment configured with
`fiji-env.sh`.

### Stop bento-cluster.
When you're ready to call it a day, you can stop bento-cluster by
running:

> `bento stop`

This will shutdown the HBase, MapReduce, and HDFS clusters managed by
bento-cluster.

Quickstart
----------

To continue using Fiji, consult the online
[quickstart guide](http://www.fiji.org/getstarted/#Quick_Start_Guide).

Upgrade Server Check-in
------------------------
Fiji BentoBox will periodically check in with an upgrade server to
determine if there are any upgrades available for your distribution.
If upgrades are available, the `fiji` script that comes with BentoBox
will periodically remind you (once a day) of available upgrades.
BentoBox sends anonymized information when checking in with the
upgrade server.

To disable checking in with the upgrade server, write a file named
`.disable_fiji_checkin` to the `$HOME/.fiji`. Using the `touch` command
is sufficient. For
example:

> `touch $HOME/.fiji/.disable_fiji_checkin`

will disable check in with the upgrade server.

