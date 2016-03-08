---
layout: post
title: FreshFijiTableReader
categories: [userguides, scoring, 0.15.0]
tags : [scoring-ug]
order : 6
version : devel
description: Description of FreshFijiTableReader API.
---

<div id="accordion-container">
  <h2 class="accordion-header"> FreshFijiTableReader.java </h2>
    <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/fijiproject/fiji-scoring/raw/fiji-scoring-root-0.15.0/src/main/java/org/fiji/scoring/FreshFijiTableReader.java"> </script>
  </div>
</div>

<h3 style="margin-top:0px;padding-top:10px;"> FreshFijiTableReader </h3>
`FreshFijiTableReader` is the primary interface for receiving freshened data. `FreshFijiTableReader` implements and extends the contract of a regular `FijiTableReader` to include the possibility of freshening. A `FreshFijiTableReader` cannot open table scanners. The `FreshFijiTableReader` implementation in FijiScoring runs Fresheners locally in-process; to run scoring remotely, check out the [FijiScoringServer](https://github.com/fijiproject/fiji-scoring-server?source=c).

While the `FreshFijiTableReader` is a `FijiTableReader`, it comes with several special options:

1. Timeouts. `FreshFijiTableReader` read requests are bound to return within a configurable timeout whether freshening has completed or not. If freshening has not completed when time expires, the reader will return partially refreshed or entirely stale data according to the partial freshening configuration. Defaults to 100 milliseconds.
2. Partial Freshening. A read request to a `FreshFijiTableReader` may include multiple columns which require freshening. All Fresheners applicable to a request are run in parallel and any new scores they produce are committed to the Fiji table according to the policy defined by this option. If partial freshening is enabled, each Freshener will commit its writes immediately when it is finished, which may leave the table in a partially refreshed state. If partial freshening is disabled, writes from all Fresheners are cached in the reader until the last Freshener finishes so that the writes may be committed atomically, guaranteeing an entirely fresh or entirely stale view at all times. Defaults to not allow partial freshening.
3. Reread period. A `FreshFijiTableReader` runs Fresheners from configuration retrieved from the Fiji meta table. This configuration may be modified while a `FreshFijiTableReader` is running in a long-lived process. In order to reflect these changes a reader can be configured to periodically read new configuration from the Fiji meta table. Defaults to never automatically reread. Freshener configuration can be reread manually at any time by calling the reader’s `rereadFreshenerRecords` method, regardless of the state of this option.
4. Columns to freshen. By default a `FreshFijiTableReader` will build and run Fresheners for all configurations it finds in the Fiji meta table. This can impose a heavy cost on the process running the reader, and may lead to errors if classes are not available. In order to avoid this problem, a reader may be configured to only read Freshener configuration for a specific set of columns. If the configured set of columns to freshen includes a column family, all Fresheners attached to columns in that family will be run. Defaults to include all columns.
5. Statistics gathering. A `FreshFijiTableReader` is capable of gathering statistics about the performance of Fresheners it runs and logging those statistics. Statistics include the percent of runs of each Freshener which return fresh or stale, percent of runs which time out, and mean and logarithmic histogram of running times. The verbosity of statistics and the logging interval are both configurable. Defaults to gather no statistics and never log.
6. Custom thread executor. Internally a `FreshFijiTableReader` uses many threads and Java `Futures` to perform asynchronous calculation. By default threads are managed by a singleton `ExecutorService`, but a user who wishes to control the behavior of the thread pool may provide an `ExecutorService` which will override the singleton.

`FreshFijiTableReader` also allows for some optional parameters on a per-request basis:

1. Timeout. If a single request has different expected behavior or constraints than the typical request made to a given reader, you can override the timeout which will be enforced on that read only. This can be used to increase the probability that a read returns newly refreshed data or to enforce a stricter SLA on the return of data under certain circumstances.
2. Request-local parameters. `FreshenerContext`s expose configuration parameters to all Freshener methods run in response to a request. Normally these parameters are populated from the configuration record for the Freshener. In order to customize the behavior of a Freshener on the fly these parameters may be overridden with this option.

In addition to extending the contract of `FijiTableReader` for read requests, `FreshFijiTableReader` provides several new methods. `rereadFreshenerRecords` and `rereadFreshenerRecords(List<FijiColumnName>)` instruct the reader to read Freshener configuration from the Fiji meta table. Specifying a list of columns will permanently change the columns which the reader will attempt to refresh. `getStatistics` returns all statistics gathered by the reader according to the configured statistics gathering options specified at construction time.
