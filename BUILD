# -*- mode: python; coding: utf-8 -*-
#
# FijiBuild v2 build descriptor
#

# Default version ID to use when emitting Maven artifacts:
# All artifacts are currently pegged on a single version ID.
with open('FIJI_VERSION', 'r') as fiji_version_file:
    maven_artifact_version = fiji_version_file.read().strip()

# --------------------------------------------------------------------------------------------------
# Python base libraries

# PEP-302 (pkg_resources) allows for namespace packages:
python_library(
    name="//python:pep-302",
    sources=[
        source("//python/pep-302/src/main/python", "pkg_resources.py"),
    ],
)

python_library(
    name="//python:base",
    sources=[
        source("//python-base/src/main/python", "**/*.py"),
        source("//python-base/src/main/python", "base/VERSION"),
    ],
    deps=["//python:pep-302"],
)

python_test(
    name="//python:base-test",
    sources=[
        source("//python-base/src/test/python", "**/*.py"),
        source("//python-base/src/test/python", "base/package/resource.txt"),
    ],
    deps=["//python:base"],
)


python_library(
    name="//python:workflow",
    sources=[source("//python-workflow/src/main/python", "**/*.py")],
    deps=["//python:base"],
)

python_test(
    name="//python:workflow-test",
    sources=[source("//python-workflow/src/test/python", "**/*.py")],
    deps=["//python:workflow"],
)

python_library(
    name="//python:avro",
    sources=[
        source("//avro/src/main/python", "**/*.py"),

        # Resources to include for IPC support:
        source("//avro/src/main/python", "avro/HandshakeRequest.avsc"),
        source("//avro/src/main/python", "avro/HandshakeResponse.avsc"),

        # Obsolete but necessary for now:
        source("//avro/src/main/python", "avro/VERSION.txt"),
    ],
)

python_test(
    name="//python:avro-test",
    sources=[
        source("//avro/src/test/python", "**/*.py"),
        source("//avro/src/test/python", "avro/interop.avsc"),
    ],
    deps=["//python:avro"],
)

# Avro CLI to inspect Avro data files:
python_binary(
    name="//python:avro-tool",
    sources=[source("//avro/src/main/scripts", "avro/avro_tool.py")],
    main_module="avro.avro_tool",
    deps=["//python:avro"],
)

# --------------------------------------------------------------------------------------------------
# DevTools

python_library(
    name="//devtools:devtools-lib",
    sources=[
        source("//devtools/src/main/python", "**/*.py"),
        source("//devtools/src/main/python", "wibi/devtools/VERSION"),
    ],
    deps=[
        "//python:base",
        "//python:workflow",
    ],
)

python_binary(
    name="//devtools:dump-record",
    main_module="wibi.scripts.dump_record",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:java-linker",
    main_module="wibi.scripts.java_linker",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:loader",
    main_module="wibi.scripts.loader",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:fiji-build",
    main_module="wibi.scripts.fiji_build",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:fiji-review",
    main_module="wibi.scripts.fiji_review",
    deps=["//devtools:devtools-lib"],
)

python_binary(
    name="//devtools:reviewboard",
    main_module="wibi.scripts.reviewboard",
    deps=["//devtools:devtools-lib"],
)

python_test(
    name="//devtools:devtools-test",
    sources=[source("//devtools/src/test/python", "**/*.py")],
    deps=["//devtools:devtools-lib"],
)

java_binary(
    name="//devtools:checkstyle",
    main_class="com.puppycrawl.tools.checkstyle.Main",
    deps=[
        maven("com.puppycrawl.tools:checkstyle:6.1.1"),
    ],
)

java_binary(
    name="//devtools:findbugs",
    main_class="edu.umd.cs.findbugs.FindBugs2",
    deps=[
        maven("com.google.code.findbugs:findbugs:3.0.0"),
    ],
)

python_binary(
    name="//devtools:flake8",
    main_module="flake8.entrypoint",
    sources=[
        source("//devtools/src/main/scripts", "flake8/entrypoint.py"),
    ],
    deps=[
        pypi("flake8", "2.3.0"),
    ],
)

python_binary(
    name="//devtools:delinearize",
    main_module="wibi.scripts.delinearize",
    deps=["//devtools:devtools-lib"],
)

# --------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------
# Checkstyle globals:

checkstyle_rules_fiji = "//.workspace_config/checkstyle/fiji_checkstyle.xml"
checkstyle_rules_wibi = "//.workspace_config/checkstyle/wibi_checkstyle.xml"
checkstyle_header_fiji = "//.workspace_config/checkstyle/fiji_header.txt"
checkstyle_header_wibi = "//.workspace_config/checkstyle/wibi_header.txt"
checkstyle_empty_suppressions = "//.workspace_config/checkstyle/empty_suppressions.xml"
checkstyle_test_suppressions = "//.workspace_config/checkstyle/test_suppressions.xml"

checkstyle_fiji = checkstyle(
    config=checkstyle_rules_fiji,
    suppressions=checkstyle_empty_suppressions,
    header=checkstyle_header_fiji,
)
checkstyle_fiji_test = checkstyle(
    config=checkstyle_rules_fiji,
    suppressions=checkstyle_test_suppressions,
    header=checkstyle_header_fiji,
)
checkstyle_wibi = checkstyle(
    config=checkstyle_rules_wibi,
    suppressions=checkstyle_empty_suppressions,
    header=checkstyle_header_wibi,
)
checkstyle_wibi_test = checkstyle(
    config=checkstyle_rules_wibi,
    suppressions=checkstyle_test_suppressions,
    header=checkstyle_header_wibi,
)

# --------------------------------------------------------------------------------------------------
# Scalastyle globals:

scalastyle_fiji = "//.workspace_config/scalastyle/fiji_config.xml"
scalastyle_wibi = "//.workspace_config/scalastyle/wibi_config.xml"

# --------------------------------------------------------------------------------------------------
# pom.xml generation globals:

# This should be used in generated_pom build definitions as a pom_template for projects that have
# scala sources.
scala_pom_template="//.workspace_config/maven/scala-pom.xml"

# --------------------------------------------------------------------------------------------------
# Maven dependencies:

avro = maven(group="org.apache.avro", id="avro", version=avro_version)
avro_ipc = maven(group="org.apache.avro", id="avro-ipc", version=avro_version)
avro_mapred = maven(
    group="org.apache.avro", id="avro-mapred", classifier="hadoop2", version=avro_version,
)

cascading_core = "cascading:cascading-core:2.6.1"
cascading_hadoop = "cascading:cascading-hadoop:2.6.1"
cascading_local = "cascading:cascading-local:2.6.1"
cascading_kryo = "cascading.kryo:cascading.kryo:0.4.7"
commons_codec = "commons-codec:commons-codec:1.6"
commons_compress = "org.apache.commons:commons-compress:1.4.1"
commons_io = "commons-io:commons-io:2.1"
commons_lang = "commons-lang:commons-lang:2.6"
commons_lang3 = "org.apache.commons:commons-lang3:3.0"
commons_pool = "commons-pool:commons-pool:1.6"
dropwizard_core="io.dropwizard:dropwizard-core:0.7.1"
dropwizard_testing = "io.dropwizard:dropwizard-testing:0.7.1"
gson = "com.google.code.gson:gson:2.2.2"
guava = "com.google.guava:guava:15.0"
httpclient = "org.apache.httpcomponents:httpclient:4.2.3"
fasterxml_jackson_module_jaxb_annotations = "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.3.3"
fasterxml_jackson_core = "com.fasterxml.jackson.core:jackson-core:2.3.3"
fasterxml_jackson_databind = "com.fasterxml.jackson.core:jackson-databind:2.3.3"
fasterxml_jackson_annotations = "com.fasterxml.jackson.core:jackson-annotations:2.3.3"
fasterxml_jackson_module_scala = "com.fasterxml.jackson.module:jackson-module-scala_2.10:2.3.3"
joda_convert = "org.joda:joda-convert:1.3.1"
joda_time = "joda-time:joda-time:2.3"
jsr305 = "com.google.code.findbugs:jsr305:1.3.9"
kryo = "com.esotericsoftware.kryo:kryo:2.21"
netty = "io.netty:netty:3.9.0.Final"
scala_compiler = "org.scala-lang:scala-compiler:2.10.4"
scala_jline = "org.scala-lang:jline:2.10.4"
scala_reflect = "org.scala-lang:scala-reflect:2.10.4"
scalatest = "org.scalatest:scalatest_2.10:2.0"
scalding_args = "com.twitter:scalding-args_2.10:0.9.1"
scalding_core = "com.twitter:scalding-core_2.10:0.9.1"
scallop = "org.rogach:scallop_2.10:0.9.5"
slf4j_api = "org.slf4j:slf4j-api:1.7.5"
slf4j_log4j12 = "org.slf4j:slf4j-log4j12:1.7.5"
spark_core = "org.apache.spark:spark-core_2.10:1.2.0-cdh5.3.5"
spark_mllib = "org.apache.spark:spark-mllib_2.10:1.2.0-cdh5.3.5"
specs2 = "org.specs2:specs2_2.10:2.3.8"
solr_core = "org.apache.solr:solr-core:4.10.3"
solr_solrj = "org.apache.solr:solr-solrj:4.10.3"

jackson_core_asl = "org.codehaus.jackson:jackson-core-asl:1.9.13"
jackson_jaxrs = "org.codehaus.jackson:jackson-jaxrs:1.9.13"
jackson_mapper_asl = "org.codehaus.jackson:jackson-mapper-asl:1.9.13"
jackson_xc = "org.codehaus.jackson:jackson-xc:1.9.13"

json_simple = "com.googlecode.json-simple:json-simple:1.1"

riemann_java_client = "com.aphyr:riemann-java-client:0.2.10"
dropwizard_metrics_core = "io.dropwizard.metrics:metrics-core:3.1.0"
dropwizard_metrics_jvm = "io.dropwizard.metrics:metrics-jvm:3.1.0"

latency_utils = "org.latencyutils:LatencyUtils:2.0.2"

# Maven dependencies for tests:
easymock = "org.easymock:easymock:3.0"
hamcrest = "org.hamcrest:hamcrest-all:1.1"
junit = "junit:junit:4.10"

# --------------------------------------------------------------------------------------------------

java_library(
    name="//com/moz/fiji/deps:jackson",
    deps=[
        maven(jackson_core_asl),
        maven(jackson_jaxrs),
        maven(jackson_mapper_asl),
        maven(jackson_xc),
    ],
)

java_library(
    name="//com/moz/fiji/deps:riemann-java-client",
    deps=[
        maven(riemann_java_client),
    ],
    maven_exclusions=[
        "io.netty:netty:*:*:*",
        "com.yammer.metrics:metrics-core:*:*:*",
        "com.codahale.metrics:metrics-core:*:*:*",
    ],
)

# --------------------------------------------------------------------------------------------------
# Internal dependencies

java_library(
    name="//testing:test-annotation-collector",
    sources=["//testing/test-collector/src/main/java"],
    deps=[
        maven(guava),
        maven(junit),
    ],
    checkstyle=checkstyle_wibi,
)

generated_pom(
    name="//testing:test-annotation-collector-pom",
    pom_name="//testing:test-annotation-collector",
    pom_file="//testing/test-collector/pom.xml",
    main_deps=["//testing:test-annotation-collector"],
)

java_library(
    name="//testing:test-runner",
    sources=["//testing/test-runner/src/main/java"],
    deps=[
        maven(guava),
        maven(junit),
        maven(slf4j_log4j12),
        "//com/moz/fiji/common:fiji-common-flags",
    ],
    checkstyle=checkstyle_wibi,
)

# --------------------------------------------------------------------------------------------------
# Fake HBase

scala_library(
    name="//com/moz/fiji/testing:fake-hbase",
    sources=[
        "//fake-hbase/src/main/java",
        "//fake-hbase/src/main/scala",
    ],
    deps=[
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.0-platform"),
        maven(easymock),
    ],
)

scala_test(
    name="//com/moz/fiji/testing:fake-hbase-test",
    sources=["//fake-hbase/src/test/scala"],
    deps=["//com/moz/fiji/testing:fake-hbase"],
)

generated_pom(
    name="//com/moz/fiji/testing:fake-hbase-pom",
    pom_name="//com/moz/fiji/testing:fake-hbase",
    pom_file="//fake-hbase/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/testing:fake-hbase"],
    test_deps=["//com/moz/fiji/testing:fake-hbase-test"],
)

# --------------------------------------------------------------------------------------------------
# Platforms

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.1-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.hbase:hbase:0.92.1-cdh4.1.4"),
#         maven("org.apache.zookeeper:zookeeper:3.4.3-cdh4.1.4"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.2-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.hbase:hbase:0.94.2-cdh4.2.2"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.2.2"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.3-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.hbase:hbase:0.94.6-cdh4.3.2"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.3.2"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.4-platform",
#     deps=[
#         maven("org.apache.hadoop:hadoop-client:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.hadoop:hadoop-core:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.hbase:hbase:0.94.6-cdh4.4.0"),
#         maven("org.apache.zookeeper:zookeeper:3.4.5-cdh4.4.0"),
#         maven("org.apache.curator:curator-recipes:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

java_library(
    name="//com/moz/fiji/platforms:cdh5.0-platform",
    deps=[
        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.3.0-cdh5.0.3"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.3.0-cdh5.0.3"),
        maven("org.apache.hadoop:hadoop-common:2.3.0-cdh5.0.3"),
        maven("org.apache.hbase:hbase-client:0.96.1.1-cdh5.0.3"),
        maven("org.apache.hbase:hbase-server:0.96.1.1-cdh5.0.3"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.0.3"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
    ],
    provides=["fiji_platform"],
)

java_library(
    name="//com/moz/fiji/platforms:cdh5.1-platform",
    deps=[
        "//com/moz/fiji/deps:jackson",

        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.3.0-cdh5.1.3"),
        maven("org.apache.hadoop:hadoop-common:2.3.0-cdh5.1.3"),
        maven("org.apache.hbase:hbase-client:0.98.1-cdh5.1.3"),
        maven("org.apache.hbase:hbase-server:0.98.1-cdh5.1.3"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.1.3"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
    ],
    provides=["fiji_platform"],
)

java_library(
    name="//com/moz/fiji/platforms:cdh5.2-platform",
    deps=[
        "//com/moz/fiji/deps:jackson",
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.5.0-cdh5.2.1"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.5.0-cdh5.2.1"),
        maven("org.apache.hadoop:hadoop-common:2.5.0-cdh5.2.1"),
        maven("org.apache.hbase:hbase-client:0.98.6-cdh5.2.1"),
        maven("org.apache.hbase:hbase-server:0.98.6-cdh5.2.1"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.2.1"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
        "org.apache.hbase:hbase-common:*:tests:*",
    ],
    provides=["fiji_platform"],
)

java_library(
    name="//com/moz/fiji/platforms:cdh5.3-platform",
    deps=[
        "//com/moz/fiji/deps:jackson",
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),
        maven("org.apache.hadoop:hadoop-mapreduce-client-jobclient:2.5.0-cdh5.3.5"),
        maven("org.apache.hadoop:hadoop-mapreduce-client-app:2.5.0-cdh5.3.5"),
        maven("org.apache.hadoop:hadoop-common:2.5.0-cdh5.3.5"),
        maven("org.apache.hbase:hbase-client:0.98.6-cdh5.3.5"),
        maven("org.apache.hbase:hbase-server:0.98.6-cdh5.3.5"),
        maven("org.apache.zookeeper:zookeeper:3.4.5-cdh5.3.5"),
        maven("org.apache.curator:curator-recipes:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
        "org.apache.hadoop:hadoop-hdfs:test-jar:*:*",
        "org.apache.hbase:hbase-common:*:tests:*",
    ],
    provides=["fiji_platform"],
)

java_library(
    name="//com/moz/fiji/platforms:compile-platform",
    deps=["//com/moz/fiji/platforms:cdh5.3-platform"],
)


# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.1-test-platform",
#     deps=["//com/moz/fiji/platforms:cdh4.1-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.1.4"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.2-test-platform",
#     deps=["//com/moz/fiji/platforms:cdh4.2-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.2.2"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.3-test-platform",
#     deps=["//com/moz/fiji/platforms:cdh4.3-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.3.2"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/platforms:cdh4.4-test-platform",
#     deps=["//com/moz/fiji/platforms:cdh4.4-platform"],
#     deps=[
#         maven("org.apache.hadoop:hadoop-minicluster:2.0.0-mr1-cdh4.4.0"),
#         maven("org.apache.curator:curator-test:2.4.1"),
#     ],
#     provides=["fiji_platform"],
# )

java_library(
    name="//com/moz/fiji/platforms:cdh5.1-test-platform",
    deps=[
        # "//com/moz/fiji/deps:jackson",  # still needed?
        "//com/moz/fiji/platforms:cdh5.1-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.3.0-cdh5.1.3"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.1-cdh5.1.3"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.1-cdh5.1.3"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//com/moz/fiji/platforms:cdh5.2-test-platform",
    deps=[
        # "//com/moz/fiji/deps:jackson",  # still needed?
        "//com/moz/fiji/platforms:cdh5.2-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.5.0-cdh5.2.1"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.6-cdh5.2.1"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.6-cdh5.2.1"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//com/moz/fiji/platforms:cdh5.3-test-platform",
    deps=[
        # "//com/moz/fiji/deps:jackson",  # still needed?
        "//com/moz/fiji/platforms:cdh5.3-platform",
        maven(guava),

        maven("org.apache.hadoop:hadoop-yarn-server-tests:test-jar:tests:2.5.0-cdh5.3.5"),

        # for HBaseTestingUtility
        maven("org.apache.hbase:hbase-server:test-jar:tests:0.98.6-cdh5.3.5"),

        # for HBaseCommonTestingUtility
        maven("org.apache.hbase:hbase-common:test-jar:tests:0.98.6-cdh5.3.5"),

        maven("org.apache.curator:curator-test:2.4.1"),
    ],
    maven_exclusions=[
        # Globally exclude Hadoop MR1:
        "org.apache.hadoop:hadoop-core:*:*:*",
    ],
)

java_library(
    name="//com/moz/fiji/platforms:test-platform",
    deps=["//com/moz/fiji/platforms:cdh5.3-test-platform"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//com/moz/fiji/hadoop:hadoop-configurator",
    sources=["//hadoop-configurator/src/main/java"],
    deps=[
        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_fiji,
)

java_test(
    name="//com/moz/fiji/hadoop:hadoop-configurator-test",
    sources=["//hadoop-configurator/src/test/java"],
    deps=[
        "//com/moz/fiji/hadoop:hadoop-configurator",
        maven(junit),
        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/hadoop:hadoop-configurator-pom",
    pom_name="//com/moz/fiji/hadoop:hadoop-configurator",
    pom_file="//hadoop-configurator/pom.xml",
    main_deps=["//com/moz/fiji/hadoop:hadoop-configurator"],
    test_deps=["//com/moz/fiji/hadoop:hadoop-configurator-test"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//com/moz/fiji/annotations:annotations",
    sources=["//annotations/src/main/java"],
)

generated_pom(
    name="//com/moz/fiji/annotations:annotations-pom",
    pom_name="//com/moz/fiji/annotations:annotations",
    pom_file="//annotations/pom.xml",
    main_deps=["//com/moz/fiji/annotations:annotations"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//com/moz/fiji/delegation:fiji-delegation",
    sources=["//fiji-delegation/src/main/java"],
    deps=[
        maven(slf4j_api),
        "//com/moz/fiji/annotations:annotations",
    ],
    checkstyle=checkstyle_fiji,
)

java_test(
    name="//com/moz/fiji/delegation:fiji-delegation-test",
    sources=["//fiji-delegation/src/test/java"],
    resources=["//fiji-delegation/src/test/resources/"],
    deps=[
        maven(junit),
        "//com/moz/fiji/delegation:fiji-delegation",
    ],
    checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/delegation:fiji-delegation-pom",
    pom_name="//com/moz/fiji/delegation:fiji-delegation",
    pom_file="//fiji-delegation/pom.xml",
    main_deps=["//com/moz/fiji/delegation:fiji-delegation"],
    test_deps=["//com/moz/fiji/delegation:fiji-delegation-test"],
)

# --------------------------------------------------------------------------------------------------

java_library(
    name="//com/moz/fiji/common:fiji-common-flags",
    sources=["//fiji-common-flags/src/main/java"],
    resources=["//fiji-common-flags/src/main/resources/"],
    deps=[
        maven(guava),
        maven(slf4j_api),
        "//com/moz/fiji/delegation:fiji-delegation",
    ],
    # TODO: Checkstyle was never run on this project. Needs corresponding cleanup before this is
    # re-enabled.
    # checkstyle=checkstyle_fiji,
)

java_test(
    name="//com/moz/fiji/common:fiji-common-flags-test",
    sources=["//fiji-common-flags/src/test/java",],
    resources=["//fiji-common-flags/src/test/resources",],
    deps=["//com/moz/fiji/common:fiji-common-flags"],
    # TODO: Checkstyle was never run on this project. Needs corresponding cleanup before this is
    # re-enabled.
    # checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/common:fiji-common-flags-pom",
    pom_name="//com/moz/fiji/common:fiji-common-flags",
    pom_file="//fiji-common-flags/pom.xml",
    main_deps=["//com/moz/fiji/common:fiji-common-flags"],
    test_deps=["//com/moz/fiji/common:fiji-common-flags-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiCommons

java_library(
    name="//com/moz/fiji/commons:fiji-commons-java",
    sources=["//fiji-commons/fiji-commons-java/src/main/java"],
    deps=[
        avro,
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),
        maven(fasterxml_jackson_module_jaxb_annotations),

        "//com/moz/fiji/annotations:annotations",
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/commons:fiji-commons-monitoring",
    sources=["//fiji-commons/fiji-commons-monitoring/src/main/java"],
    deps=[
        maven(guava),
        maven(slf4j_api),
        maven(jsr305),

        maven(dropwizard_metrics_core),
        maven(dropwizard_metrics_jvm),
        maven(latency_utils),

        "//com/moz/fiji/commons:fiji-commons-java",
        "//com/moz/fiji/deps:riemann-java-client",
    ],
    checkstyle=checkstyle_fiji,
)

scala_library(
    name="//com/moz/fiji/commons:fiji-commons-scala",
    sources=["//fiji-commons/fiji-commons-scala/src/main/scala"],
    deps=[
        maven(guava),
        maven(fasterxml_jackson_module_scala),
        maven(jsr305),
        maven(slf4j_api),

        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/commons:fiji-commons-java",
    ],
    scalastyle=scalastyle_fiji,
)

java_library(
    name="//com/moz/fiji/checkin:fiji-checkin",
    sources=["//fiji-checkin/src/main/java"],
    deps=[
        maven(commons_io),
        maven(commons_lang),
        maven(gson),
        maven(guava),
        maven(httpclient),
        maven(slf4j_api),

        "//com/moz/fiji/common:fiji-common-flags",
    ],
    checkstyle=checkstyle_fiji,
)

avro_java_library(
    name="//com/moz/fiji/commons:fiji-commons-java-test-avro-lib",
    sources=["//fiji-commons/fiji-commons-java/src/test/avro/*.avdl"],
)

java_test(
    name="//com/moz/fiji/commons:fiji-commons-java-test",
    jvm_args=["-Dcom.moz.fiji.commons.ResourceTracker.tracking_level=REFERENCES"],
    sources=["//fiji-commons/fiji-commons-java/src/test/java"],
    deps=[
        "//com/moz/fiji/commons:fiji-commons-java",
        "//com/moz/fiji/commons:fiji-commons-java-test-avro-lib",
    ],
    checkstyle=checkstyle_fiji_test,
)

java_test(
    name="//com/moz/fiji/commons:fiji-commons-monitoring-test",
    sources=["//fiji-commons/fiji-commons-monitoring/src/test/java"],
    deps=[
        "//com/moz/fiji/commons:fiji-commons-monitoring",
    ],
    checkstyle=checkstyle_fiji_test,
)

scala_test(
    name="//com/moz/fiji/commons:fiji-commons-scala-test",
    sources=["//fiji-commons/fiji-commons-scala/src/test/scala"],
    deps=[
        maven(junit),
        "//com/moz/fiji/commons:fiji-commons-scala",
        "java_library(//com/moz/fiji/commons:fiji-commons-java-test)",
    ],
)

# TODO: Re-enable this once we have support for setting jar MANIFEST.mf files. TestVersionInfo
#     relies on being able to read the "package title" from the MANIFEST.mf in the fiji-delegation
#     jar.
java_test(
    name="//com/moz/fiji/checkin:fiji-checkin-test",
    sources=["//fiji-checkin/src/test/java"],
    deps=[
        maven(easymock),
        "//com/moz/fiji/checkin:fiji-checkin",
    ],
    checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/commons:fiji-commons-java-pom",
    pom_name="//com/moz/fiji/commons:fiji-commons-java",
    pom_file="//fiji-commons/fiji-commons-java/pom.xml",
    main_deps=["//com/moz/fiji/commons:fiji-commons-java"],
    test_deps=["//com/moz/fiji/commons:fiji-commons-java-test"],
)

generated_pom(
    name="//com/moz/fiji/commons:fiji-commons-monitoring-pom",
    pom_name="//com/moz/fiji/commons:fiji-commons-monitoring",
    pom_file="//fiji-commons/fiji-commons-monitoring/pom.xml",
    main_deps=["//com/moz/fiji/commons:fiji-commons-monitoring"],
    test_deps=["//com/moz/fiji/commons:fiji-commons-monitoring-test"],
)

generated_pom(
    name="//com/moz/fiji/commons:fiji-commons-scala-pom",
    pom_name="//com/moz/fiji/commons:fiji-commons-scala",
    pom_file="//fiji-commons/fiji-commons-scala/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/commons:fiji-commons-scala"],
    test_deps=["//com/moz/fiji/commons:fiji-commons-scala-test"],
)

generated_pom(
    name="//com/moz/fiji/checkin:fiji-checkin-pom",
    pom_name="//com/moz/fiji/checkin:fiji-checkin",
    pom_file="//fiji-checkin/pom.xml",
    main_deps=["//com/moz/fiji/checkin:fiji-checkin"],
    test_deps=["//com/moz/fiji/checkin:fiji-checkin-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiSolr

#java_library(
#    name="//com/moz/fiji/solr:fiji-solr-lib",
#    sources=["//fiji-solr/src/main/java"],
#    deps=[
#        maven(slf4j_api),
#        maven(solr_core),
#        maven(solr_solrj),
#        "//com/moz/fiji/schema:fiji-schema",
#    ],
#    checkstyle=checkstyle_fiji,
#)

#java_binary(
#    name="//com/moz/fiji/solr:fiji-solr",
#    main_class="com.moz.fiji.solr.HelloWorld",
#    deps=[
#        "//com/moz/fiji/solr:fiji-solr-lib",
#    ],
#    maven_exclusions=[],
#)

#java_test(
#    name="//com/moz/fiji/solr:fiji-solr-test",
#    sources=["//fiji-solr/src/test/java"],
#    deps=[
#        maven(junit),
#        "//com/moz/fiji/solr:fiji-solr-lib",
#    ],
#    checkstyle=checkstyle_fiji_test,
#)

#generated_pom(
#    name="//com/moz/fiji/solr:fiji-solr-pom",
#    pom_name="//com/moz/fiji/solr:fiji-solr",
#    pom_file="//fiji-solr/pom.xml",
#    main_deps=["//com/moz/fiji/solr:fiji-solr-lib"],
#    test_deps=["//com/moz/fiji/solr:fiji-solr-test"],
#)

# --------------------------------------------------------------------------------------------------
# FijiSchema

avro_java_library(
    name="//com/moz/fiji/schema:fiji-schema-avro",
    sources=[
        "//fiji-schema/fiji-schema/src/main/avro/Layout.avdl",
        "//fiji-schema/fiji-schema/src/main/avro/Security.avdl",
    ],
)

java_library(
    name="//com/moz/fiji/schema:schema-platform-api",
    sources=["//fiji-schema/platform-api/src/main/java"],
    deps=[
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/delegation:fiji-delegation",

        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_fiji,
)

# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/schema:cdh41mr1-bridge",
#     sources=["//fiji-schema/cdh41mr1-bridge/src/main/java"],
#     resources=["//fiji-schema/cdh41mr1-bridge/src/main/resources/"],
#     deps=[
#         "//com/moz/fiji/annotations:annotations",
#         "//com/moz/fiji/schema:schema-platform-api",
#
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.1-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_library(
#     name="//com/moz/fiji/schema:cdh42mr1-bridge",
#     sources=["//fiji-schema/cdh42mr1-bridge/src/main/java"],
#     resources=["//fiji-schema/cdh42mr1-bridge/src/main/resources/"],
#     deps=[
#         "//com/moz/fiji/annotations:annotations",
#         "//com/moz/fiji/schema:schema-platform-api",
#
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.2-platform"),
#     ],
# )

java_library(
    name="//com/moz/fiji/schema:cdh5-bridge",
    sources=["//fiji-schema/cdh5-bridge/src/main/java"],
    resources=["//fiji-schema/cdh5-bridge/src/main/resources/"],
    deps=[
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/schema:schema-platform-api",

        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.3-platform"),
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/schema:fiji-schema",
    sources=["//fiji-schema/fiji-schema/src/main/java"],
    resources=["//fiji-schema/fiji-schema/src/main/resources/"],
    deps=[
        avro,
        maven(commons_pool),
        maven(gson),
        maven(guava),
        maven(jsr305),
        maven(slf4j_api),

        "//com/moz/fiji/schema:fiji-schema-avro",
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/checkin:fiji-checkin",
        "//com/moz/fiji/common:fiji-common-flags",
        "//com/moz/fiji/commons:fiji-commons-java",
        "//com/moz/fiji/delegation:fiji-delegation",

        "//com/moz/fiji/schema:schema-platform-api",  # brings compile-platform
        "//com/moz/fiji/schema:cdh5-bridge",          # brings cdh5.3-platform
        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_fiji,
        suppressions="//fiji-schema/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_fiji,
    ),
)

java_binary(
    name="//com/moz/fiji/schema:fiji",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
    ],
)

# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//com/moz/fiji/schema:fiji-cdh4.1",
#     main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
#     deps=[
#         "//com/moz/fiji/schema:fiji-schema",
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.1-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//com/moz/fiji/schema:fiji-cdh4.2",
#     main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
#     deps=[
#         "//com/moz/fiji/schema:fiji-schema",
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.2-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//com/moz/fiji/schema:fiji-cdh4.3",
#     main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
#     deps=[
#         "//com/moz/fiji/schema:fiji-schema",
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.3-platform"),
#     ],
# )
# DEPRECATED. Uncomment if you need to use cdh4.
# java_binary(
#     name="//com/moz/fiji/schema:fiji-cdh4.4",
#     main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
#     deps=[
#         "//com/moz/fiji/schema:fiji-schema",
#         dynamic(fiji_platform="//com/moz/fiji/platforms:cdh4.4-platform"),
#     ],
# )

java_binary(
    name="//com/moz/fiji/schema:fiji-cdh5.0",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.0-platform"),
    ],
)

java_binary(
    name="//com/moz/fiji/schema:fiji-cdh5.1",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.1-platform"),
    ],
)

java_binary(
    name="//com/moz/fiji/schema:fiji-cdh5.2",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.2-platform"),
    ],
)

java_binary(
    name="//com/moz/fiji/schema:fiji-cdh5.3",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.3-platform"),
    ],
)

java_binary(
    name="//com/moz/fiji/schema:fiji-dynamic",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        dynamic(fiji_platform=None),
        "//com/moz/fiji/schema:fiji-schema",
    ],
)

avro_java_library(
    name="//com/moz/fiji/schema:fiji-schema-test-avro",
    sources=[
        "//fiji-schema/fiji-schema/src/test/avro/*.avdl",
        "//fiji-schema/fiji-schema/src/test/avro/*.avsc",
    ],
)

java_test(
    name="//com/moz/fiji/schema:fiji-schema-test",
    sources=["//fiji-schema/fiji-schema/src/test/java"],
    resources=["//fiji-schema/fiji-schema/src/test/resources/"],
    deps=[
        maven(easymock),
        maven(hamcrest),
        maven(junit),

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
        "//com/moz/fiji/testing:fake-hbase",

        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-test-avro",
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_fiji,
        suppressions="//fiji-schema/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_fiji,
    ),
)

scala_library(
    name="//com/moz/fiji/schema:fiji-schema-extras",
    sources=[
        "//fiji-schema/fiji-schema-extras/src/main/java",
        "//fiji-schema/fiji-schema-extras/src/main/scala",
    ],
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
    ],
)

scala_test(
    name="//com/moz/fiji/schema:fiji-schema-extras-test",
    sources=["//fiji-schema/fiji-schema-extras/src/test/scala"],
    deps=[
        "//com/moz/fiji/testing:fake-hbase",
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-extras",

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//com/moz/fiji/schema:schema-platform-api-pom",
    pom_name="//com/moz/fiji/schema:schema-platform-api",
    pom_file="//fiji-schema/platform-api/pom.xml",
    main_deps=["//com/moz/fiji/schema:schema-platform-api"],
)

generated_pom(
    name="//com/moz/fiji/schema:cdh5-bridge-pom",
    pom_name="//com/moz/fiji/schema:cdh5-bridge",
    pom_file="//fiji-schema/cdh5-bridge/pom.xml",
    main_deps=["//com/moz/fiji/schema:cdh5-bridge"],
)

generated_pom(
    name="//com/moz/fiji/schema:fiji-schema-pom",
    pom_name="//com/moz/fiji/schema:fiji-schema",
    pom_file="//fiji-schema/fiji-schema/pom.xml",
    main_deps=["//com/moz/fiji/schema:fiji-schema"],
    test_deps=["//com/moz/fiji/schema:fiji-schema-test"],
)

generated_pom(
    name="//com/moz/fiji/schema:fiji-schema-extras-pom",
    pom_name="//com/moz/fiji/schema:fiji-schema-extras",
    pom_file="//fiji-schema/fiji-schema-extras/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/schema:fiji-schema-extras"],
    test_deps=["//com/moz/fiji/schema:fiji-schema-extras-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiSchema DDL Shell

scala_library(
    name="//com/moz/fiji/schema:fiji-schema-shell-lib",
    sources=["//fiji-schema-shell/src/main/scala"],
    resources=["//fiji-schema-shell/src/main/resources"],
    deps=[
        maven(commons_lang3),
        maven(scala_jline),

        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-extras",
    ],
)

java_binary(
    name="//com/moz/fiji/schema:fiji-schema-shell",
    main_class="com.moz.fiji.schema.shell.ShellMain",
    deps=[
        "//com/moz/fiji/schema:fiji-schema-shell-lib",
    ],
)

avro_java_library(
    name="//com/moz/fiji/schema:fiji-schema-shell-test-avro-lib",
    sources=["//fiji-schema-shell/src/test/avro/*.avdl"],
)

scala_test(
    name="//com/moz/fiji/schema:fiji-schema-shell-test",
    sources=["//fiji-schema-shell/src/test/scala"],
    resources=["//fiji-schema-shell/src/test/resources"],
    deps=[
        maven(scalatest),
        maven(specs2),

        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "//com/moz/fiji/schema:fiji-schema-shell-test-avro-lib",
        "//com/moz/fiji/schema:fiji-schema-shell-lib",

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//com/moz/fiji/schema:fiji-schema-shell-pom",
    pom_name="//com/moz/fiji/schema:fiji-schema-shell-lib",
    pom_file="//fiji-schema-shell/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/schema:fiji-schema-shell-lib"],
    test_deps=["//com/moz/fiji/schema:fiji-schema-shell-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiMR

avro_java_library(
    name="//com/moz/fiji/mapreduce:fiji-mapreduce-avro-lib",
    sources=["//fiji-mapreduce/fiji-mapreduce/src/main/avro/*.avdl"],
)

java_library(
    name="//com/moz/fiji/mapreduce:platform-api",
    sources=["//fiji-mapreduce/platform-api/src/main/java"],
    deps=[
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/delegation:fiji-delegation",

        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/mapreduce:cdh5-mrbridge",
    sources=["//fiji-mapreduce/cdh5-bridge/src/main/java"],
    resources=["//fiji-mapreduce/cdh5-bridge/src/main/resources/"],
    deps=[
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/delegation:fiji-delegation",
        "//com/moz/fiji/mapreduce:platform-api",

        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.3-platform"),
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/mapreduce:fiji-mapreduce",
    sources=["//fiji-mapreduce/fiji-mapreduce/src/main/java"],
    resources=["//fiji-mapreduce/fiji-mapreduce/src/main/resources"],
    deps=[
        maven(slf4j_api),
        avro_mapred,

        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-extras",
        "//com/moz/fiji/mapreduce:platform-api",
        "//com/moz/fiji/mapreduce:fiji-mapreduce-avro-lib",

        "//com/moz/fiji/mapreduce:cdh5-mrbridge",          # brings cdh5.3-platform

        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_fiji,
        suppressions="//fiji-mapreduce/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_fiji,
    ),
)

avro_java_library(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-avro-lib",
    sources=[
        "//fiji-mapreduce-lib/fiji-mapreduce-lib/src/main/avro/*.avdl",
        "//fiji-mapreduce-lib/fiji-mapreduce-lib/src/main/avro/*.avsc",
    ],
)

java_library(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib",
    sources=["//fiji-mapreduce-lib/fiji-mapreduce-lib/src/main/java"],
    resources=["//fiji-mapreduce-lib/fiji-mapreduce-lib/src/main/resources"],
    deps=[
        maven(commons_codec),
        maven(commons_io),
        maven(commons_lang),
        maven(gson),
        maven(guava),
        maven(jsr305),

        "//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-avro-lib",
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/mapreduce:fiji-mapreduce",

        "//com/moz/fiji/hadoop:hadoop-configurator",
    ],
    checkstyle=checkstyle_fiji,
)

scala_library(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext",
    sources=["//fiji-mapreduce-lib/schema-shell-ext/src/main/scala"],
    resources=["//fiji-mapreduce-lib/schema-shell-ext/src/main/resources"],
    deps=[
        maven(gson),
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-shell-lib",
        "//com/moz/fiji/mapreduce:fiji-mapreduce",
        "//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib",
    ],
)

scala_test(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext-test",
    sources=[
        "//fiji-mapreduce-lib/schema-shell-ext/src/test/scala",
        "//fiji-mapreduce-lib/schema-shell-ext/src/test/java",
    ],
    resources=["//fiji-mapreduce-lib/schema-shell-ext/src/test/resources"],
    deps=[
        maven(easymock),
        maven(scalatest),
        maven(specs2),
        "//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext",

        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/schema:fiji-schema-shell-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/mapreduce:fiji-mapreduce-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-test)",  # FIXME: extract fiji test framework
    ],
)

java_binary(
    name="//com/moz/fiji/mapreduce:fijimr",
    main_class="com.moz.fiji.schema.tools.FijiToolLauncher",
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/mapreduce:fiji-mapreduce",
    ],
)

# TODO: This produces test failures right now.
java_test(
    name="//com/moz/fiji/mapreduce:fiji-mapreduce-test",
    sources=["//fiji-mapreduce/fiji-mapreduce/src/test/java"],
    resources=["//fiji-mapreduce/fiji-mapreduce/src/test/resources"],
    deps=[
        maven(easymock),
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "//com/moz/fiji/mapreduce:fiji-mapreduce",

        "//com/moz/fiji/mapreduce:cdh5-mrbridge",          # brings cdh5.3-platform

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_fiji_test,
)

java_test(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-test",
    sources=["//fiji-mapreduce-lib/fiji-mapreduce-lib/src/test/java"],
    resources=["//fiji-mapreduce-lib/fiji-mapreduce-lib/src/test/resources"],
    deps=[
        maven("org.apache.mrunit:mrunit:jar:hadoop2:1.1.0"),
        "//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib",

        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/mapreduce:fiji-mapreduce-test)",  # FIXME: extract fiji test framework

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/mapreduce:platform-api-pom",
    pom_name="//com/moz/fiji/mapreduce:platform-api",
    pom_file="//fiji-mapreduce/platform-api/pom.xml",
    main_deps=["//com/moz/fiji/mapreduce:platform-api"],
)

generated_pom(
    name="//com/moz/fiji/mapreduce:cdh5-bridge-pom",
    pom_name="//com/moz/fiji/mapreduce:cdh5-bridge",
    pom_file="//fiji-mapreduce/cdh5-bridge/pom.xml",
    main_deps=["//com/moz/fiji/mapreduce:cdh5-mrbridge"],
)

generated_pom(
    name="//com/moz/fiji/mapreduce:fiji-mapreduce-pom",
    pom_name="//com/moz/fiji/mapreduce:fiji-mapreduce",
    pom_file="//fiji-mapreduce/fiji-mapreduce/pom.xml",
    main_deps=["//com/moz/fiji/mapreduce:fiji-mapreduce"],
    test_deps=["//com/moz/fiji/mapreduce:fiji-mapreduce-test"],
)

generated_pom(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-pom",
    pom_name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib",
    pom_file="//fiji-mapreduce-lib/fiji-mapreduce-lib/pom.xml",
    main_deps=["//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib"],
    test_deps=["//com/moz/fiji/mapreduce/lib:fiji-mapreduce-lib-test"],
)

generated_pom(
    name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext-pom",
    pom_name="//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext",
    pom_file="//fiji-mapreduce-lib/schema-shell-ext/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext"],
    test_deps=["//com/moz/fiji/mapreduce/lib:fiji-mapreduce-schema-shell-ext-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiHive

java_library(
    name="//com/moz/fiji/hive:fiji-hive-lib",
    sources=["//fiji-hive-adapter/fiji-hive-adapter/src/main/java"],
    deps=[
        # TODO: Move this to a platform perhaps?
        maven("org.apache.hive:hive-exec:0.12.0-cdh5.0.3"),
        maven("org.apache.hive:hive-serde:0.12.0-cdh5.0.3"),

        "//com/moz/fiji/schema:fiji-schema",
    ],
    maven_exclusions=[
        "junit:junit:*:*:*",
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/hive:fiji-hive-tools",
    sources=["//fiji-hive-adapter/fiji-hive-tools/src/main/java"],
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
    ],
    checkstyle=checkstyle_fiji,
)

java_test(
    name="//com/moz/fiji/hive:fiji-hive-test",
    sources=["//fiji-hive-adapter/fiji-hive-adapter/src/test/java"],
    deps=[
        # Don't use the avro-1.7.5-cdh5.0.3 dependency since it causes the fiji-schema test avro jar
        # to fail to load.
        avro,

        "//com/moz/fiji/hive:fiji-hive-lib",
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle(
        config=checkstyle_rules_fiji,
        suppressions="//fiji-hive-adapter/build-resources/resources/src/main/checkstyle/suppressions.xml",
        header=checkstyle_header_fiji,
    ),
)

java_test(
    name="//com/moz/fiji/hive:fiji-hive-tools-test",
    sources=["//fiji-hive-adapter/fiji-hive-tools/src/test/java"],
    deps=[
        "//com/moz/fiji/hive:fiji-hive-tools",
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_fiji_test,
)

generated_pom(
    name="//com/moz/fiji/hive:fiji-hive-pom",
    pom_name="//com/moz/fiji/hive:fiji-hive-lib",
    pom_file="//fiji-hive-adapter/fiji-hive-adapter/pom.xml",
    main_deps=["//com/moz/fiji/hive:fiji-hive-lib"],
    test_deps=["//com/moz/fiji/hive:fiji-hive-test"],
)

generated_pom(
    name="//com/moz/fiji/hive:fiji-hive-tools-pom",
    pom_name="//com/moz/fiji/hive:fiji-hive-tools",
    pom_file="//fiji-hive-adapter/fiji-hive-tools/pom.xml",
    main_deps=["//com/moz/fiji/hive:fiji-hive-tools"],
    test_deps=["//com/moz/fiji/hive:fiji-hive-tools-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiExpress

avro_java_library(
    name="//com/moz/fiji/express:fiji-express-avro-lib",
    sources=[
        "//fiji-express/fiji-express/src/main/avro/*.avdl",
    ],
)

scala_library(
    name="//com/moz/fiji/express:fiji-express-lib",
    sources=[
        "//fiji-express/fiji-express/src/main/java",
        "//fiji-express/fiji-express/src/main/scala",
    ],
    resources=["//fiji-express/fiji-express/src/main/resources"],
    deps=[
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/deps:riemann-java-client",
        "//com/moz/fiji/express:fiji-express-avro-lib",
        "//com/moz/fiji/mapreduce:fiji-mapreduce",
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-shell-lib",
        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
        maven("com.google.protobuf:protobuf-java:2.5.0"),
        maven("com.twitter.elephantbird:elephant-bird-core:4.4"),
        maven("com.twitter.elephantbird:elephant-bird-hadoop-compat:4.4"),
        maven(cascading_core),
        maven(cascading_hadoop),
        maven(cascading_kryo),
        maven(cascading_local),
        maven(fasterxml_jackson_module_scala),
        maven(kryo),
        maven(scalding_args),
        maven(scalding_core),
    ],
)

java_binary(
    name="//com/moz/fiji/express:fiji-express",
    main_class="com.moz.fiji.express.flow.ExpressTool",
    deps=[
        "//com/moz/fiji/express:fiji-express-lib",
    ],
)

avro_java_library(
    name="//com/moz/fiji/express:fiji-express-test-avro-lib",
    sources=[
        "//fiji-express/fiji-express/src/test/avro/*.avdl",
    ],
)

# TODO: These tests take forever. Add support for native-libs (jvm properties).
scala_test(
    name="//com/moz/fiji/express:fiji-express-test",
    # Exclude FijiSuite and SerDeSuite since these are traits.
    test_name_pattern=".*(?<!^Fiji)(?<!SerDe)Suite$",
    sources=["//fiji-express/fiji-express/src/test/scala"],
    resources=["//fiji-express/fiji-express/src/test/resources"],
    deps=[
        maven(scalatest),
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "//com/moz/fiji/schema:fiji-schema-shell-lib",
        "//com/moz/fiji/express:fiji-express-test-avro-lib",
        "//com/moz/fiji/express:fiji-express-lib",
        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

scala_library(
    name="//com/moz/fiji/express:fiji-express-examples",
    sources=["//fiji-express/fiji-express-examples/src/main/scala"],
    resources=["//fiji-express/fiji-express-examples/src/main/resources"],
    deps=[
        avro,
        maven(scalding_core),
        maven(scalding_args),
        maven(cascading_core),
        "//com/moz/fiji/express:fiji-express-lib",
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
)

scala_test(
    name="//com/moz/fiji/express:fiji-express-examples-test",
    test_name_pattern=".*Suite$",
    sources=["//fiji-express/fiji-express-examples/src/test/scala"],
    resources=["//fiji-express/fiji-express-examples/src/test/resources"],
    deps=[
        maven(scalatest),
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "//com/moz/fiji/express:fiji-express-examples",
        "java_library(//com/moz/fiji/express:fiji-express-test)",  # FIXME: extract fiji test framework
        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

scala_library(
    name="//com/moz/fiji/express:fiji-express-tools",
    sources=["//fiji-express/fiji-express-tools/src/main/scala"],
    deps=[
        maven(scala_compiler),
        maven(scala_jline),
        maven(scala_reflect),
        avro,
        maven(guava),
        maven(scalding_core),
        maven(scalding_args),
        maven(cascading_core),
        "//com/moz/fiji/annotations:annotations",
        "//com/moz/fiji/schema:fiji-schema",
        "//com/moz/fiji/schema:fiji-schema-shell",
        "//com/moz/fiji/mapreduce:fiji-mapreduce",
        "//com/moz/fiji/express:fiji-express-lib",
    ],
)

scala_test(
    name="//com/moz/fiji/express:fiji-express-tools-test",
    test_name_pattern=".*Suite$",
    sources=["//fiji-express/fiji-express-tools/src/test/scala"],
    deps=[
        maven(scalatest),
        "//com/moz/fiji/testing:fake-hbase",
        "//com/moz/fiji/express:fiji-express-tools",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/express:fiji-express-test)",  # FIXME: extract fiji test framework
        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//com/moz/fiji/express:fiji-express-pom",
    pom_name="//com/moz/fiji/express:fiji-express-lib",
    pom_file="//fiji-express/fiji-express/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/express:fiji-express-lib"],
    test_deps=["//com/moz/fiji/express:fiji-express-test"],
)

generated_pom(
    name="//com/moz/fiji/express:fiji-express-examples-pom",
    pom_name="//com/moz/fiji/express:fiji-express-examples",
    pom_file="//fiji-express/fiji-express-examples/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/express:fiji-express-examples"],
    test_deps=["//com/moz/fiji/express:fiji-express-examples-test"],
)

generated_pom(
    name="//com/moz/fiji/express:fiji-express-tools-pom",
    pom_name="//com/moz/fiji/express:fiji-express-tools",
    pom_file="//fiji-express/fiji-express-tools/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/express:fiji-express-tools"],
    test_deps=["//com/moz/fiji/express:fiji-express-tools-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiSpark

scala_library(
    name="//com/moz/fiji/deps:spark-core",
    deps=[
        maven(spark_core),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        # This is a temporary hack.
        "joda-time:joda-time:*:*:[2.2,)",

    ],
    provides=["spark_core"]
)

scala_library(
    name="//com/moz/fiji/deps:spark-mllib",
    deps=[
        maven(spark_mllib),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        # This is a temporary hack.
        "joda-time:joda-time:*:*:[2.2,)",

    ],
    provides=["spark_mllib"]
)

scala_library(
    name="//com/moz/fiji/spark:fiji-spark",
    sources=["//fiji-spark/src/main/scala"],
    deps=[
        "//com/moz/fiji/schema:fiji-schema",
        dynamic(spark_core="//com/moz/fiji/deps:spark-core"),
        maven("joda-time:joda-time:2.6"),
        # CDH 5.3 platform will be the first version to support FijiSpark
        # as its the first release to include Spark 1.2.x
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.3-platform"),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        "joda-time:joda-time:*:*:[2.2,)",
    ]
)

scala_test(
    name="//com/moz/fiji/spark:fiji-spark-test",
    test_name_pattern=".*Suite$",
    sources=["//fiji-spark/src/test/scala"],
    deps=[
        "//com/moz/fiji/spark:fiji-spark",
        "//com/moz/fiji/schema:fiji-schema",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",
        "//com/moz/fiji/commons:fiji-commons-scala",
        maven(spark_core),
        maven("joda-time:joda-time:2.6"),
        maven(scalatest),
        # CDH 5.3 platform will be the first version to support FijiSpark
        # as its the first release to include Spark 1.2.x
        dynamic(fiji_platform="//com/moz/fiji/platforms:cdh5.3-platform"),
    ],
    maven_exclusions=[
        "io.netty:netty-all:*:*:*",
        # Version ranges are not currently supported in KBv2
        "joda-time:joda-time:*:*:[2.2,)",
    ]
)

generated_pom(
    name="//com/moz/fiji/spark:fiji-spark-pom",
    pom_name="//com/moz/fiji/spark:fiji-spark",
    pom_file="//fiji-spark/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/spark:fiji-spark"],
    test_deps=["//com/moz/fiji/spark:fiji-spark-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiModeling

scala_library(
    name="//com/moz/fiji/modeling:fiji-modeling",
    sources=["//fiji-modeling/fiji-modeling/src/main/scala"],
    deps=["//com/moz/fiji/express:fiji-express-lib"],
)

avro_java_library(
    name="//com/moz/fiji/modeling:fiji-modeling-examples-avro-lib",
    sources=["//fiji-modeling/fiji-modeling-examples/src/main/avro/*.avdl"],
)

scala_library(
    name="//com/moz/fiji/modeling:fiji-modeling-examples",
    sources=["//fiji-modeling/fiji-modeling-examples/src/main/scala"],
    deps=[
        "//com/moz/fiji/express:fiji-express-lib",
        "//com/moz/fiji/modeling:fiji-modeling",
        "//com/moz/fiji/modeling:fiji-modeling-examples-avro-lib",

        dynamic(fiji_platform="//com/moz/fiji/platforms:compile-platform"),
    ],
)

scala_test(
    name="//com/moz/fiji/modeling:fiji-modeling-test",
    test_name_pattern=".*Suite$",
    sources=["//fiji-modeling/fiji-modeling/src/test/scala"],
    deps=[
        maven(easymock),
        maven(scalatest),
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/schema:fiji-schema-shell-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/express:fiji-express-test)",  # FIXME: extract fiji test framework
        "//com/moz/fiji/modeling:fiji-modeling",

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

scala_test(
    name="//com/moz/fiji/modeling:fiji-modeling-examples-test",
    sources=["//fiji-modeling/fiji-modeling-examples/src/test/scala"],
    resources=["//fiji-modeling/fiji-modeling-examples/src/test/resources"],
    deps=[
        maven(scalatest),
        "//com/moz/fiji/modeling:fiji-modeling-examples",

        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/schema:fiji-schema-shell-test)",  # FIXME: extract fiji test framework
        "java_library(//com/moz/fiji/express:fiji-express-test)",  # FIXME: extract fiji test framework

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

generated_pom(
    name="//com/moz/fiji/modeling:fiji-modeling-pom",
    pom_name="//com/moz/fiji/modeling:fiji-modeling",
    pom_file="//fiji-modeling/fiji-modeling/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/modeling:fiji-modeling"],
    test_deps=["//com/moz/fiji/modeling:fiji-modeling-test"],
)

generated_pom(
    name="//com/moz/fiji/modeling:fiji-modeling-examples-pom",
    pom_name="//com/moz/fiji/modeling:fiji-modeling-examples",
    pom_file="//fiji-modeling/fiji-modeling-examples/pom.xml",
    pom_template=scala_pom_template,
    main_deps=["//com/moz/fiji/modeling:fiji-modeling-examples"],
    test_deps=["//com/moz/fiji/modeling:fiji-modeling-examples-test"],
)

# --------------------------------------------------------------------------------------------------
# FijiREST

java_library(
    name="//com/moz/fiji/rest:fiji-rest-lib",
    sources=["//fiji-rest/fiji-rest/src/main/java"],
    deps=[
        # Something pulls in jersey 1.9.
        maven("com.sun.jersey:jersey-core:1.18.1"),
        maven("com.sun.jersey:jersey-json:1.18.1"),
        maven("com.sun.jersey.contribs:jersey-guice:1.18.1"),
        maven("javax.servlet:javax.servlet-api:3.0.1"),

        maven(dropwizard_core),
        maven("org.eclipse.jetty:jetty-servlets:9.0.7.v20131107"),

        "//com/moz/fiji/schema:fiji-schema",
    ],
    maven_exclusions=[
        "org.eclipse.jetty.orbit:javax.servlet:*:*:*",
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/rest:fiji-rest-monitoring-lib",
    sources=["//fiji-rest/monitoring/src/main/java"],
    resources=["//fiji-rest/monitoring/src/main/resources"],
    deps=[
        maven("io.dropwizard:dropwizard-metrics:0.7.1"),
        "//com/moz/fiji/commons:fiji-commons-monitoring",
    ],
    checkstyle=checkstyle_fiji,
)

java_library(
    name="//com/moz/fiji/rest:fiji-rest-standard-plugin",
    sources=["//fiji-rest/standard-plugin/src/main/java"],
    resources=["//fiji-rest/standard-plugin/src/main/resources"],
    deps=["//com/moz/fiji/rest:fiji-rest-lib"],
)

avro_java_library(
    name="//com/moz/fiji/rest:fiji-rest-standard-plugin-test-avro-lib",
    sources=["//fiji-rest/standard-plugin/src/test/avro/*.avdl"],
)

java_test(
    name="//com/moz/fiji/rest:fiji-rest-test",
    sources=["//fiji-rest/fiji-rest/src/test/java"],
    resources=["//fiji-rest/fiji-rest/src/test/resources"],
    deps=[
        maven(dropwizard_testing),

        "//com/moz/fiji/rest:fiji-rest-lib",
        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
    checkstyle=checkstyle_fiji_test,
)

java_test(
    name="//com/moz/fiji/rest:fiji-rest-monitoring-test",
    sources=["//fiji-rest/monitoring/src/test/java"],
    resources=["//fiji-rest/monitoring/src/test/resources"],
    deps=[
        "//com/moz/fiji/rest:fiji-rest-monitoring-lib",
    ],
    checkstyle=checkstyle_fiji_test,
)

java_test(
    name="//com/moz/fiji/rest:fiji-rest-standard-plugin-test",
    sources=["//fiji-rest/standard-plugin/src/test/java"],
    deps=[
        maven(guava),
        maven(dropwizard_core),
        maven(dropwizard_testing),

        "//com/moz/fiji/deps:jackson",
        "java_library(//com/moz/fiji/rest:fiji-rest-test)",
        "//com/moz/fiji/rest:fiji-rest-standard-plugin",
        "//com/moz/fiji/rest:fiji-rest-standard-plugin-test-avro-lib",

        "//com/moz/fiji/testing:fake-hbase",
        "java_library(//com/moz/fiji/schema:fiji-schema-test)",  # FIXME: extract fiji test framework

        dynamic(fiji_platform="//com/moz/fiji/platforms:test-platform"),
    ],
)

java_binary(
    name="//com/moz/fiji/rest:fiji-rest",
    main_class="com.moz.fiji.rest.FijiRESTService",
    deps=["//com/moz/fiji/rest:fiji-rest-lib"],
    maven_exclusions=[
        "org.slf4j:slf4j-log4j12:*:*:*",
        "org.slf4j:log4j-over-slf4j:*:*:*",
    ],
)

generated_pom(
    name="//com/moz/fiji/rest:fiji-rest-pom",
    pom_name="//com/moz/fiji/rest:fiji-rest-lib",
    pom_file="//fiji-rest/fiji-rest/pom.xml",
    main_deps=["//com/moz/fiji/rest:fiji-rest-lib"],
    test_deps=["//com/moz/fiji/rest:fiji-rest-test"],
)

generated_pom(
    name="//com/moz/fiji/rest:fiji-rest-standard-plugin-pom",
    pom_name="//com/moz/fiji/rest:fiji-rest-standard-plugin",
    pom_file="//fiji-rest/standard-plugin/pom.xml",
    main_deps=["//com/moz/fiji/rest:fiji-rest-standard-plugin"],
    test_deps=["//com/moz/fiji/rest:fiji-rest-standard-plugin-test"],
)

generated_pom(
    name="//com/moz/fiji/rest:fiji-rest-monitoring-pom",
    pom_name="//com/moz/fiji/rest:fiji-rest-monitoring",
    pom_file="//fiji-rest/monitoring/pom.xml",
    main_deps=["//com/moz/fiji/rest:fiji-rest-monitoring-lib"],
    test_deps=["//com/moz/fiji/rest:fiji-rest-monitoring-test"],
)

# TODO: custom versions of fiji-rest for different platforms
