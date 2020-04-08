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

// scalastyle:off println

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.bench.ml.LogisticRegressionDataGenerator.Params
import org.rogach.scallop.ScallopConf

object LogisticRegression {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
   LR: an example app for Linear Regression

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
  }


  def main(args: Array[String]): Unit = {

    val params = new Params(args)

    var inputPath = params.dataPath

    val conf = new SparkConf()
      .setAppName("LogisticRegressionWithLBFGS")
    Common.setMaster(conf)

    val sc = new SparkContext(conf)

    // $example on$
    // Load training data in LIBSVM format.
    val data: RDD[LabeledPoint] = sc.objectFile(inputPath)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val accuracy = predictionAndLabels.filter(x => x._1 == x._2).count().toDouble / predictionAndLabels.count()
    println(s"Accuracy = $accuracy")

    sc.stop()
  }
}
// scalastyle:on println
