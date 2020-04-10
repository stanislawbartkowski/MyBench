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
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.rogach.scallop.ScallopConf

object RandomForestClassification {

  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner(
      """
    RF: an example app.

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val onumClasses = opt[Int]("numClasses", required = true)
    private val omaxDepth = opt[Int]("maxDepth", required = true)
    private val omaxBins = opt[Int]("maxBins", required = true)
    private val onumTress = opt[Int]("numTrees", required = true)
    private val ofeatureSubsetStrategy = opt[String]("featureSubsetStrategy", required = true)
    private val oimpurity = opt[String]("impurity", required = true)

    verify()
    val dataPath: String = odataPath.getOrElse("")
    val numClasses: Int = onumClasses.getOrElse(2)
    val maxDepth: Int = omaxDepth.getOrElse(30)
    val maxBins: Int = omaxBins.getOrElse(32)
    val numTrees: Int = onumTress.getOrElse(3)
    val featureSubsetStrategy: String = ofeatureSubsetStrategy.getOrElse("auto")
    val impurity: String = oimpurity.getOrElse("gini")
  }

  def main(args: Array[String]) {
    val params = new Params(args)
    run(params)
  }

  private def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"RFC with $params")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.
    val data: RDD[LabeledPoint] = sc.objectFile(params.dataPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.

    val categoricalFeaturesInfo = Map[Int, Int]()

    val model = RandomForest.trainClassifier(trainingData, params.numClasses, categoricalFeaturesInfo,
      params.numTrees, params.featureSubsetStrategy, params.impurity, params.maxDepth, params.maxBins)

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
