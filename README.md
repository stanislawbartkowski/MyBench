# MyBench

My own version of https://github.com/Intel-bigdata/HiBench.

# Installation

## Install sbt
https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html

## Clone and create package
> git clone https://github.com/stanislawbartkowski/MyBench.git<br>
> cd BenchmarkProj<br>
> sbt assembly<br>

Test<br>
> ll target/scala-2.11/BenchJar.jar<br>
```
-rw-r--r--. 1 ambari-qa hadoop 1379091 Apr 10 23:53 target/scala-2.11/BenchJar.jar
```
It running *sbt* on the target machine is not possible, run *sbt* on other Linux machine and copy target/scala-2.11/BenchJar.jar to the target host.

## Customization
>cd conf<br>
> cp custom.rc.template custom.rc<br>

The MyBench test is controlled by *test.rc* environment settings. Any variable in *test.tc* can be overwritten by *custom.rc*
<br>
The basic customization:<br>
 * *TESTLIST* List of tests to be executed. *TESTLIST* variable in *test.rc* can be used as a reference, it contains a list of all tests implemented so far. *TESTLIST* in *custom.rc* is the list of tests to be executed in the current run. If the variable in *custom.rc* is entirely commented out, all tests are executed.
 * *BENCHSIZE* Size of the test. Possible values: *tiny,small,large,huge,gigantic,bigdata*. Start with *tiny* and being sure that *MyBench* is running, climb up the ladder.
 
**HBase Phoenix**<br>
* ZOOKEEPER variable: set ZOOKEEPER variable having the hostname of Zookeeper cluster. It is necessary to run HBase Phoenix command line tool.
<br>
> /usr/hdp/current/phoenix-client/bin//usr/hdp/current/phoenix-client $ZOOKEEPER /script file/<br>
<br>

* Increase "Phoenix Query Timeout" config parameters from default 1 minute. For instance: 10 minutes.
 
## Privileges

### HDFS
The MyBench temporary HDFS space is determined by *TMPBASEDIR* variable, default is */tmp/bench*
### Hive
If Hive *hive.server2.enable.doAs* impersonation is not set:
 *  Give *hive* user *read/write/execute* permissions to *TMPBASEDIR* HDFS directory.<br>
 
If impersonation is enabled:
 * User running MyBench test should have permissions to */warehouse/tablespace* directory.<br>

Also, set *hive.strict.managed.tables* to false to allow creation non-transactional tables managed by Hive.
<br>
### HBase, Phoenix
As *hbase* user, create additional *bench* namespace<br>
> hbase shell<br>
> create_namespace 'bench'

Give the user running MyBench test, the full authority in *SYSTEM.\** and *BENCH.\** namespace.
### HBase, Phoenix client
Make sure that client can connect to Hbase Phoenix using Zookeeper servers provided.<br>
 > /usr/hdp/current/phoenix-client/bin/sqlline.py  data3-worker.cloudga.com,data1-worker.cloudga.com,data2-worker.cloudga.com:2181/hbase-secure <br>
 ```
 Setting property: [incremental, false]
Setting property: [isolation, TRANSACTION_READ_COMMITTED]
issuing: !connect jdbc:phoenix:data3-dev.cloudga.com,mgmt1-dev.cloudga.com,mgmt2-dev.cloudga.com:2181/hbase-secure none none org.apache.phoenix.jdbc.PhoenixDriver
Connecting to jdbc:phoenix:data3-dev.cloudga.com,mgmt1-dev.cloudga.com,mgmt2-dev.cloudga.com:2181/hbase-secure
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/hdp/3.1.0.0-78/phoenix/phoenix-5.0.0.3.1.0.0-78-client.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/hdp/3.1.0.0-78/hadoop/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
20/08/07 12:09:04 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Connected to: Phoenix (version 5.0)
Driver: PhoenixEmbeddedDriver (version 5.0)
Autocommit status: true
Transaction isolation: TRANSACTION_READ_COMMITTED
Building list of tables and columns for tab-completion (set fastconnect to true to skip)...
133/133 (100%) Done
Done
sqlline version 1.2.0
0: jdbc:phoenix:data3-worker.cloudga.com,data1-w> 
 ```
 Troubleshooting<br>
 In order to make *sqlline.py* client software more verbose, enable TRACING. This setting is not managed by Ambari and needs to be done manually.
>vi /usr/hdp/3.1.0.0-78/phoenix/bin/log4j.properties
```
....
#psql.root.logger=WARN,console
psql.root.logger=TRACE,console
 ```
### Kerberos
If the cluster is Kerberized, obtain valid Kerberos ticket before running the test.

# Run test suite
> ./runtest.sh<br>

Executes all tests specified in the *TESTLIST* variable in *conf/custom.rc* source file. If the *TESTLIST* variable is commented out, all tests are executed.




