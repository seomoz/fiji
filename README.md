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
