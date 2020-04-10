# MyBench

My own version of https://github.com/Intel-bigdata/HiBench.

# Installation

## Install sbt
https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html

## Clone and make package
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
 * *TESTLIST* List of tests to be executed. *TESTLIST* variable in *test.rc* contains list of all tests implemented so far. *TESTLIST* can contain list of test separated by comma or single test.
<br>
 * *BENCHSIZE* Size of the test. Currently only *tiny* is supported






