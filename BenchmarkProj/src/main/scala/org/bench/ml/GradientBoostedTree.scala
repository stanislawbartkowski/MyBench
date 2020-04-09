package org.bench.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.rogach.scallop.ScallopConf

object GradientBoostedTree {

  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""
    GBT: an example of Gradient Boosted Tree for classification

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val onumClasses = opt[Int]("numClasses", required = true)
    private val omaxDepth = opt[Int]("maxDepth", required = true)
    private val omaxBins = opt[Int]("maxBins", required = true)
    private val onumIterations = opt[Int]("numIterations", required = true)
    private val olearningRate = opt[Double]("learningRate", required = true)
    verify()
    val dataPath : String = odataPath.getOrElse("")
    val numClasses: Int = onumClasses.getOrElse(2)
    val maxDepth: Int = omaxDepth.getOrElse(30)
    val maxBins: Int = omaxBins.getOrElse(32)
    val numIterations: Int = onumIterations.getOrElse(20)
    val learningRate: Double = olearningRate.getOrElse(0.1)
  }


  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    run(params)
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"Gradient Boosted Tree with $params")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numClasses = params.numClasses
    val maxDepth = params.maxDepth
    val maxBins = params.maxBins
    val numIterations = params.numIterations
    val learningRate = params.learningRate

    // Load  data file.
    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = numIterations
    boostingStrategy.learningRate = learningRate
    boostingStrategy.treeStrategy.numClasses = numClasses
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.maxBins = maxBins
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)

    sc.stop()
  }
}

