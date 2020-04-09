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
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

/**
 * :: DeveloperApi ::
 * Generate sample data for Linear Regression. This class
 * generates uniformly random values for each feature and
 * adds Gaussian noise with mean 'eps' to the label 'Y'.
 */
object LinearRegressionDataGenerator {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
   LR: an example data generator for Linear Regression

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    val onumExamples = opt[Int]("numExamples", required = true)
    val onumFeatures = opt[Int]("numFeatures", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
    val numExamples = onumExamples.getOrElse(1000)
    val numFeatures: Int = onumFeatures.getOrElse(50)
  }


  /**
   * Generate an RDD containing sample data for Linear Regression.
   *
   * @param sc          SparkContext to use for creating the RDD.
   * @param numExamples Number of examples that will be contained in the RDD.
   * @param numFeatures Numer of features to gnerate for each example.
   * @param eps         Epsilon factor by which examples are scaled.
   * @param numParts    Number of partitions of the generated RDD. Default value is 3.
   * @param seed        Random seed for each partition
   */
  def generateLinearRDD(
                         sc: SparkContext,
                         numExamples: Int,
                         numFeatures: Int,
                         eps: Double,
                         numParts: Int = 3,
                         seed: Long = System.currentTimeMillis()): RDD[LabeledPoint] = {
    val random = new Random()
    // Random values distributed uniformly in [-0.5, 0.5]
    val weights = Array.fill(numFeatures)(random.nextDouble() - 0.5)

    val data: RDD[LabeledPoint] = sc.parallelize(0 until numExamples, numParts).mapPartitions {
      part =>
        val rnd = new Random(seed)
        // mean for each feature
        val xMean = Array.fill[Double](weights.length)(0.0)
        // variance for each feature
        val xVariance = Array.fill[Double](weights.length)(1.0 / 3.0)

        def rndElement(i: Int) = {
          (rnd.nextDouble() - 0.5) * math.sqrt(12.0 * xVariance(i)) + xMean(i)
        }

        part.map { _ =>
          val features = Vectors.dense(weights.indices.map {
            rndElement(_)
          }.toArray)
          val label = blas.ddot(weights.length, weights, 1, features.toArray, 1) + eps * rnd.nextGaussian()
          LabeledPoint(label, features)
        }
    }
    data
  }

  def main(args: Array[String]) {
    val params = new Params(args)

    val conf = new SparkConf().setAppName("LinearRegressionDataGenerator")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    var outputPath = params.dataPath
    var numExamples = params.numExamples
    var numFeatures: Int = params.numFeatures
    val numPartitions = Common.getNumOfPartitons(sc)
    var eps: Double = 1.0

    val data = generateLinearRDD(sc, numExamples, numFeatures, eps, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
