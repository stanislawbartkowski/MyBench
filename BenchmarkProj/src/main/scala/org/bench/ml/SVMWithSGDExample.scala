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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.rogach.scallop.ScallopConf

object SVMWithSGDExample {

  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""
    SparseNaiveBayes: an example naive Bayes app for LIBSVM data.

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val onumIterations =  opt[Int]("numIterations",required = true)
    private val ostepSize  = opt[Double]("stepSize",required = true)
    private val oregParam = opt[Double]("regParam",required = true)
    verify()
    val dataPath : String = odataPath.getOrElse("")
    val numIterations: Int = onumIterations.getOrElse(100)
    val stepSize: Double = ostepSize.getOrElse(1.0)
    val regParam: Double = oregParam.getOrElse(0.01)
  }

  def main(args: Array[String]): Unit = {
    val params = new Params(args)

    run(params)
  }

  def run(params: Params): Unit = {

    val conf = new SparkConf().setAppName(s"SVM with $params")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numIterations = params.numIterations
    val stepSize = params.stepSize
    val regParam = params.regParam

    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = SVMWithSGD.train(training, numIterations, stepSize, regParam)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    sc.stop()
  }
}
