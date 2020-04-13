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
## Customization
>cd conf<br>
> cp custom.rc.template custom.rc<br>

The MyBench test is controlled by *test.rc* environment settings. Any variable in *test.tc* can be overwritten by *custom.rc*
<br>
The basic customization:<br>
 * *TESTLIST* List of tests to be executed. *TESTLIST* variable in *test.rc* can be used as a reference, it contains a list of all tests implemented so far. *TESTLIST* in *custom.rc* is the list of tests to be executed.
 * *BENCHSIZE* Size of the test. Currently only *tiny* is supported
 
**HBase Phoenix**<br>
* ZOOKEEPER variable: set ZOOKEEPER variable having the hostname of Zookeeper cluster. It is necessary to run Phoenix command line tool
<br>
> /usr/hdp/current/phoenix-client/bin//usr/hdp/current/phoenix-client $ZOOKEEPER /script file/<br>
<br>

* Increase "Phoenix Query Timeout" config parameters from default 1 minute. For instance: 5 minutes.
 
## Privileges

### HDFS
The MyBench temporary HDFS space is determined by *TMPBASEIDIR* variable, default is */tmp/bench*
### Hive
If Hive *hive.server2.enable.doAs* impersonation is not set, give *hive* user *read/write/execute* permissions to *TMPBASEDIR* HDFS directory.<br>
If impersonation is enabled, user running MyBench test should have permissions to */warehouse/tablespace* directory.<br>
Also, *hive.strict.managed.tables* should be set to false to allow creation non-transactional tables managed by Hive.
<br>
### HBase, Phoenix
As *hbase* user, create additional *bench* namespace<br>
> hbase shell<br>
> create_namespace 'bench'

Give the user running MyBench test, the full authority in *SYSTEM.\** and *BENCH.\** namespace.
### Kerberos
If the cluster is Kerberized, obtain valid Kerberos ticket before running the test.

# Run test suite
> ./runtest.sh<br>

Executes all tests specified in the *TESTLIST* variable in *conf/custom.rc* source file.




