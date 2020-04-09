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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf


object LinearRegression {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
     LR: an example application for Linear Regression

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    val onumIterations = opt[Int]("numIterations", required = true)
    val ostepSize = opt[Double]("stepSize", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
    val numIterations: Int = onumIterations.getOrElse(100)
    val stepSize: Double = ostepSize.getOrElse(0.00000001)
  }

  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    run(params)
  }


  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"LinearRegressionWithSGD with $params")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numIterations = params.numIterations
    val stepSize = params.stepSize

    // Load training data in LabeledPoint format.
    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Building the model
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = data.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("Training Mean Squared Error = " + MSE)

    sc.stop()
  }
}
