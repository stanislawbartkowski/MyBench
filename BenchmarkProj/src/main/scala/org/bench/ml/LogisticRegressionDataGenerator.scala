package org.bench.ml

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

/**
 * :: DeveloperApi ::
 * Generate test data for LogisticRegression. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
object LogisticRegressionDataGenerator {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
   LR: an example data generator for Logistic Regression

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val onumExamples = opt[Int]("numExamples", required = true)
    private val onumFeatures = opt[Int]("numFeatures", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
    val numExamples = onumExamples.getOrElse(200000)
    val numFeatures: Int = onumFeatures.getOrElse(20)
  }


  /**
   * Generate an RDD containing test data for LogisticRegression.
   *
   * @param sc        SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps       Epsilon factor by which positive examples are scaled.
   * @param nparts    Number of partitions of the generated RDD. Default value is 2.
   * @param probOne   Probability that a label is 1 (and not 0). Default value is 0.5.
   */
  def generateLogisticRDD(
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

  def main(args: Array[String]) {
    val params = new Params(args)

    val conf = new SparkConf().setAppName("LogisticRegressionDataGenerator")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)
    val numPartitions = Common.getNumOfPartitons(sc)

    var outputPath = params.dataPath
    var numExamples: Int = params.numExamples
    var numFeatures: Int = params.numFeatures
    val eps = 3

    val data = generateLogisticRDD(sc, numExamples, numFeatures, eps, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
