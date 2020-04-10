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

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for SVM. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
object SVMDataGenerator {

  /**
   * Generate an RDD containing test data for SVM.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param nparts Number of partitions of the generated RDD. Default value is 2.
   */
  def generateSVMRDD(
                      sc: SparkContext,
                      nexamples: Int,
                      nfeatures: Int,
                      nparts: Int = 2): RDD[LabeledPoint] = {
    val globalRnd = new Random(94720)
    val trueWeights = Array.fill[Double](nfeatures)(globalRnd.nextGaussian())
    val data: RDD[LabeledPoint] = sc.parallelize(0 until nexamples,nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val x = Array.fill[Double](nfeatures) {
        rnd.nextDouble() * 2.0 - 1.0
      }
      val yD = blas.ddot(trueWeights.length, x, 1, trueWeights, 1) + rnd.nextGaussian() * 0.1
      val y = if (yD < 0) 0.0 else 1.0
      LabeledPoint(y, Vectors.dense(x))
    }
    data
  }

  def main(args: Array[String]) {
    val params = new FeatureParams(args,200000,20,"SVM Data Generator")
    val conf = new SparkConf().setAppName("SVMDataGenerator")
    val sc = new SparkContext(conf)

    var outputPath = params.dataPath
    var numExamples: Int = params.numExamples
    var numFeatures: Int = params.numFeatures

    val numPartitions = Common.getNumOfPartitons(sc)

    val data = generateSVMRDD(sc, numExamples, numFeatures, numPartitions)
    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
