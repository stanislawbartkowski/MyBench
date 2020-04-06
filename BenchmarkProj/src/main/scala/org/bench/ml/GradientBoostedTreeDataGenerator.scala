package org.bench.ml

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

/**
 * :: DeveloperApi ::
 * Generate test data for Gradient Boosting Tree. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
object GradientBoostedTreeDataGenerator {

  /**
   * Generate an RDD containing test data for Gradient Boosting Tree.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps Epsilon factor by which positive examples are scaled.
   * @param nparts Number of partitions of the generated RDD. Default value is 2.
   * @param probOne Probability that a label is 1 (and not 0). Default value is 0.5.
   */

  def generateGBTRDD(
                      sc: SparkContext,
                      nexamples: Int,
                      nfeatures: Int,
                      eps: Double,
                      nparts: Int = 2,
                      probOne: Double = 0.5): RDD[LabeledPoint] = {
    val data = sc.parallelize(0 until nexamples, nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val y = if (idx % 2 == 0) 0.0 else 1.0
      val x = Array.fill[Double](nfeatures) {
        rnd.nextGaussian() + (y * eps)
      }
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }


  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""
    SparseNaiveBayes: an example naive Bayes app for LIBSVM data.

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val onumExamples = opt[Int]("numExamples",required = true)
    private val onumFeatures = opt[Int]("numFeatures",required = true)
    verify()
    val dataPath : String = odataPath.getOrElse("")
    var numExamples: Int = onumExamples.getOrElse(200000)
    var numFeatures: Int = onumFeatures.getOrElse(20)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GradientBoostingTreeDataGenerator")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)
    val params = new Params(args)


    val eps = 0.3
    val numPartitions = Common.getNumOfPartitons(sc)
    val outputPath = params.dataPath
    val numExamples: Int = params.numExamples
    val numFeatures: Int = params.numFeatures

    val data = generateGBTRDD(sc, numExamples, numFeatures, eps, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}

