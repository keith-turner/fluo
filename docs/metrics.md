<!---
Copyright 2014 Fluo authors (see AUTHORS)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
Monitoring Fluo
===============

Fluo core is instrumented using [dropwizard metrics][1].  This allows fluo
users to easily gather information about Fluo by configuring different
reporters.  Fluo will allways setup a JMX reporter, regardless of the number of
reporters configured.  This is done because the dropwizard config mechanism do
not currently support the JMX reporter.   The JMX reporter makes it easy to see
fluo stats in jconsole or jvisualvm.

Configuring Fluo processes
--------------------------

When starting an oracle or workers, using the `fluo` script, the
`$FLUO_CONF_DIR/metrics.yaml` file is used to configure reporters.  Consult the
[dropwizard config docs][2] inorder to learn how to populate this file.  There
is one important difference with that documentation. Because Fluo is only
leveraging the dropwizard metrics config code, you do not need the top level
`metrics:` element in your `metrics.yaml` file.  The example `metrics.yaml`
file does not have this element.

Configuring Fluo Clients
------------------------

Fluo client code that uses the basic API or map reduce API can configure
reporters by setting `io.fluo.metrics.yaml.base64` in `fluo.propeties`.  The
value of this property should be a single line base64 encoded yaml config.
This can easily be generated with the following command.  Also,
FluoConfiguration has some conveince methods for setting this property.

```
cat conf/metrics.yaml | base64 -w 0
```  

The property `io.fluo.metrics.yaml.base64` is not used by processes started
with the fluo script.  The primary motivation of having this property is to
enable collection of metrics from map task executing load transaction using 
FluoOutputFormat.

In order for the `io.fluo.metrics.yaml.base64` property to work, a map reduce
job must include the `fluo-metrics` module.  This module contains the code that
configures reporters based on yaml.  The module is separate from `fluo-core`
inorder to avoid adding a lot of dependencies that are only needed when
configuring reporters.

Reporter Dependencies
---------------------

The core dropwizard metrics library has a few reporters.  However if you would
like to utilize additional reporters, then you will need to add dependencies.
For example if you wanted to use Ganglia, then you would need to depend on
specific dropwizard ganglia maven artifacts.

Custom Reporters
----------------

If a reporter follows the discovery mechanisms used by dropwizard
configuration, then it may be automatically configurable via yaml.  However
this has not been tested.

Metrics reported by Fluo
------------------------

Some of the metrics reported have the class name as the suffix.  This classname
is the observer or load task that executed the transactions.   This should
allow things like transaction collisions to be tracked per a class.  In the
table below this is denoted with `<cn>`.  In the table below `io.flou` is
shortened to `i.f`.

|Metric                                   | Description                         |
|-----------------------------------------|-------------------------------------|
|i.f.tx.lockWait.&lt;cn&gt;               | Tracks the amount of time spent waiting on locks held by other transactions |
|i.f.tx.time.&lt;cn&gt;                   | Tracks the amount of time each transaction took to execute |
|i.f.tx.collisions.&lt;cn&gt;             | Tracks the number of row/columns where collisions occurred during a transaction |
|i.f.tx.set.&lt;cn&gt;                    | Tracks the number of row/columns set by transactions |
|i.f.tx.read.&lt;cn&gt;                   | Tracks the number of row/columns read by transactions |
|i.f.tx.locks.timedout.&lt;cn&gt;         | Tracks the number of locks rolled back because of timeout.  These are locks that are held for very long periods by another transaction that appears to be alive based on zookeeper |
|i.f.tx.locks.dead.&lt;cn&gt;             | Tracks the number of locks rolled back because of a dead transactor.  These are locks held by a process that appears to be dead according to zookeeper |
|i.f.tx.status.&lt;status&gt;.&lt;cn&gt;  | Tracks the counts for the different ways in which a transaction can terminate |
|i.f.oracle.client.rpc.getStamps.time     | Tracks time that RPC calls to oracle from client take |
|i.f.oracle.client.stamps                 | Tracks the number of timestamps allocated by this client |
|i.f.oracle.server.stamps                 | Tracks the number of timestamps allocated by the oracle |
|i.f.oracle.server.request                | Tracks the number of request for timestamps from clients.  Clients can request multiple timestamp in a single RPC request |

[1]: https://dropwizard.github.io/metrics/3.1.0/
[2]: https://dropwizard.github.io/dropwizard/manual/configuration.html#metrics
