package org.bench.ml

import org.rogach.scallop.ScallopConf

class FeatureParams(arguments: Seq[String], defaExamples: Int, defaFeatures: Int, banner: String) extends ScallopConf(arguments) {

  // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

  banner(
    """
    $banner

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
  private val odataPath = opt[String]("dataPath", required = true)
  private val onumExamples = opt[Int]("numExamples", required = true)
  private val onumFeatures = opt[Int]("numFeatures", required = true)
  verify()

  val dataPath: String = odataPath.getOrElse("")
  val numExamples = onumExamples.getOrElse(defaExamples)
  val numFeatures: Int = onumFeatures.getOrElse(defaFeatures)
}
