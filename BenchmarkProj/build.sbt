name := "BenchmarkProj"

version := "0.1"

scalaVersion := "2.11.8"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyJarName in assembly := "BenchJar.jar"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.2" % "provided"
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.2"

// https://mvnrepository.com/artifact/org.rogach/scallop
libraryDependencies += "org.rogach" %% "scallop" % "3.4.0"

