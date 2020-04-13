package org.bench.ml

import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf


object HdfsWordCount {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numDocs 2000 --numVocab 1000 --docLenMin 50 --docLenMax 100000

    banner(
      """
    Classical wordcount application

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val ooutputPath = opt[String]("outputPath", required = true)
    verify

    var dataPath = odataPath.getOrElse("")
    var outputPath = ooutputPath.getOrElse("")
  }

  def main(args: Array[String]) {
    val params = new Params(args)

    val conf = new SparkConf().setAppName("WordCount")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)


    val infile = params.dataPath

    val f = sc.sequenceFile(infile, classOf[Text], classOf[Text])
    val a = f.flatMap { case (x, y) => (x.toString.split(' ').union(y.toString.split(' '))).map(word => (word, 1)) }
    val wc = a.reduceByKey(_ + _)
    wc.saveAsTextFile(params.outputPath)
  }


}
