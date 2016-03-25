<!-- https://github.com/seomoz/docs/blob/improved-readme/Readme%20header.md -->

![Status Incubating][status-badge]
![Team Big Data][team-badge]
![Closed Source][open-badge]
![Not Critical][critical-badge]
[![Email][email-badge]][email-link]
[![Slack][slack-badge]][slack-link]
![Internal][scope-badge]

[status-badge]: https://img.shields.io/badge/status-incubating-blue.svg?style=flat
[team-badge]: https://img.shields.io/badge/team-big_data-green.svg?style=flat
[open-badge]: https://img.shields.io/badge/open_source-nope-orange.svg?style=flat
[critical-badge]: https://img.shields.io/badge/critical-no-lightgrey.svg?style=flat
[email-badge]: https://img.shields.io/badge/email-bigdata--dev%40moz.com-green.svg?style=flat
[email-link]: mailto:bigdata-dev@moz.com
[slack-badge]: https://img.shields.io/badge/slack-%23big--data-ff69b4.svg?style=flat
[slack-link]: https://moz.slack.com/messages/big-data/
[scope-badge]: https://img.shields.io/badge/scope-internal-lightgrey.svg?style=flat

# Fiji

This workspace contains the collection of Fiji projects.

## Prerequisites

Make sure you have the following installed:

 - Python 3.4.2 or greater.
 - Oracle JDK 7 or greater.

## Build instructions

 1. Download and extract the workspace into a directory `$wkspc`:  
    ```git clone https://github.com/seomoz/fiji $wkspc```

 2. Make sure the workspace settings are correct in:
    [`$wkspc/.workspace_config/conf.py`](https://github.com/seomoz/fiji/tree/master/.workspace_config/conf.py)

 3. From the workspace root directory, the Fiji artifacts (Maven artifacts and CLI executables) can be build by running:
    ```./bin/fiji-build build```

    The build process fetches and produces Maven artifacts into the local repository: `$wkspc/output/maven_repository/`.
    Executables are generated into `$wkspc/output/bin/`.

 4. To build jars suitable for uploads to Maven Central, see `$wkspc/devtools/prepare-central-jars`.

 5. To build assemblies, which are tar balls that package transitive dependencies for runtime use, you will
    need to put all of your repositories in the `profiles` section of your `settings.xml`. Then, in each
    directory with `pom.xml` run:
    ```mvn assembly:single -Ddescriptor=src/main/assembly/release.xml```


## Fiji CLI

Upon successful build, the Fiji CLI may be found at `$wkspc/output/bin/com/moz/fiji/schema/fiji`.
Different versions of the CLI may be found at `$wkspc/output/bin/com/moz/fiji/schema/fiji-cdh5.x`.

In order to (re)build a specific version of the CLI, you may use:
```./bin/fiji-build build //com/moz/fiji/schema:fiji-cdh5.x```.

The definition of the Fiji artifacts can be found in the BUILD descriptor in the root directory: [`$wkspc/BUILD`](https://github.com/com/moz/fiji/tree/master/BUILD).


## Running tests

In order to run tests, you can use:
```./bin/fiji-build test```

Tests involving the Hadoop Map/Reduce framework may run very slowly unless the appropriate version of the Hadoop native libraries are installed on the system.
