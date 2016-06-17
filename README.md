<!-- https://github.com/seomoz/docs/blob/improved-readme/Readme%20header.md -->

[![Build Status][build-status-badge]][build-status-link]
[![Ready Stories][tickets-badge]][tickets-link]
![Status Incubating][status-badge]
![Team Big Data][team-badge]
![Closed Source][open-badge]
![Not Critical][critical-badge]
[![Email][email-badge]][email-link]
[![Slack][slack-badge]][slack-link]
![Internal][scope-badge]

[build-status-badge]: https://travis-ci.com/seomoz/fiji.svg?branch=master
[build-status-link]: https://travis-ci.com/seomoz/fiji
[tickets-badge]: https://badge.waffle.io/seomoz/fiji.png?label=ready&title=Ready
[tickets-link]: https://waffle.io/seomoz/fiji
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

All development is done within vagrant.  You will need it and a provider, such as virtualbox or vmware fusion.

## Build instructions

```
$ cd the-checkout
$ vagrant up
$ vagrant ssh
vagrant@fiji:~$ cd fiji
vagrant@fiji:~/fiji$ ./bin/fiji-build build         # compile
vagrant@fiji:~/fiji$ ./bin/fiji-build test          # run the tests
vagrant@fiji:~/fiji$ ./devtools/create-assemblies   # create release tarballs
```

## Fiji CLI

Upon successful build, the Fiji CLI may be found at `$wkspc/output/bin/com/moz/fiji/schema/fiji`.
Different versions of the CLI may be found at `$wkspc/output/bin/com/moz/fiji/schema/fiji-cdh5.x`.

In order to (re)build a specific version of the CLI, you may use:
```./bin/fiji-build build //com/moz/fiji/schema:fiji-cdh5.x```.

The definition of the Fiji artifacts can be found in the BUILD descriptor in the root directory: [`$wkspc/BUILD`](https://github.com/com/moz/fiji/tree/master/BUILD).
