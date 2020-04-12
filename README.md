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
 
## Privileges

### HDFS
The MyBench temporary HDFS space is determined by *TMPBASEIDIR* variable, default is */tmp/bench*
### Hive
If Hive *hive.server2.enable.doAs* impersonation is not set, give *hive* user *read/write/execute* permissions to *TMPBASEDIR* HDFS directory.
### HBase, Phoenix
Give the user running the MyBench authority in *SYSTEM.* namespace.
### Kerberos
If the cluster is Kerberized, obtain valid Kerberos ticket before running the test.

# Run test suite
> ./runtest.sh<br>

Executes all tests specified in the *TESTLIST* variable in *conf/custom.rc* source file.




