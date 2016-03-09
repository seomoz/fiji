FijiMR
======

FijiMR is a framework for writing MapReduce-based computation
over data stored in FijiSchema.

For more information about FijiSchema, see
[the Fiji project homepage](http://www.fiji.org).

Further documentation is available at the Fiji project
[Documentation Portal](http://docs.fiji.org)

Issues are being tracked at [the Fiji JIRA instance](https://jira.fiji.org/browse/FIJIMR).

CDH5.0.3 Notes
--------------

* TotalOrderPartitioner is now in a new package structure which might be incompatible.
  This should be bridgeable.
* The switch to mr2 jars means a lot of mapred.* conf keys will no longer be valid or populated.
  Be suspicious of any of these found in the code. Prefer their mr2 counterparts.
* The profiles should be removed from fiji-mapreduce/pom.xml and it should just be a 'flat'
  pom.xml.
