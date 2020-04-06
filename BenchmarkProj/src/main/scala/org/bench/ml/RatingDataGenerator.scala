package org.bench.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.rogach.scallop.ScallopConf

object RatingDataGenerator {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner("""
    Rating Data Generator example

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val onumUsers = opt[Int]("numUsers",required = true)
    private val onumProducts = opt[Int]("numProducts",required = true)
    private val osparsity = opt[Double]("sparsity",required = true)
    private val oimplicitPrefs = opt[Boolean]("implicitPrefs")
    private val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
    verify()

    val dataPath : String = odataPath.getOrElse("")
    val numUsers: Int = onumUsers.getOrElse(0)
    val numProducts: Int = onumProducts.getOrElse(0);
    val implicitPrefs: Boolean = oimplicitPrefs.getOrElse(false)
    val sparsity : Double = osparsity.getOrElse(0)
  }


  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val conf = new SparkConf().setAppName("RatingDataGeneration")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

//    var outputPath = ""
//    var numUsers: Int = 100
//    var numProducts: Int = 100
//    var sparsity: Double = 0.05
//    var implicitPrefs: Boolean = false
//    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
//    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
//      .getOrElse((parallel / 2).toString).toInt

    val numPartitions = Common.getNumOfPartitons(sc)

    val rawData: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, params.numUsers, params.numProducts, numPartitions)
    val rng = new java.util.Random()
    val sparsity: Double = params.sparsity
    val data = rawData.map { v =>
      val a = Array.fill[Double](v.size)(0.0)
      v.foreachActive { (i, vi) =>
        if (rng.nextDouble <= sparsity) {
          a(i) = vi
        }
      }
      Vectors.dense(a).toSparse
    }

    data.saveAsObjectFile(params.dataPath)
  }
}