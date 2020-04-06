package org.bench.mr


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

import scala.collection.mutable

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.SparseVector

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val apples = opt[Int](required = true)
  val bananas = opt[Int]()
  val name = trailArg[String]()
  verify()
}

class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner("""
    AVL example

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
  private val odataPath = opt[String]("dataPath",required = true)
  private val onumUsers = opt[Int]("numUsers")
  private val onumProducts = opt[Int]("numProducts")
  private val okryo = opt[Boolean]("kryo")
  private val onumIterations = opt[Int]("numIterations")
  private val olambda = opt[Double]("lambda")
  private val orank = opt[Int]("rank")
  private val onumRecommends = opt[Int]("numRecommends")
  private val onumUserBlocks = opt[Int]("numUserBlocks")
  private val onumProductBlocks = opt[Int]("numProductBlocks")
  private val oimplicitPrefs = opt[Boolean]("implicitPrefs")
  private val help = opt[Boolean]("help", noshort = true, descr = "Show this message")
  verify()

  val dataPath : String = odataPath.getOrElse("")
  val numUsers: Int = onumUsers.getOrElse(0)
  val numProducts: Int = onumProducts.getOrElse(0);
  val kryo: Boolean = okryo.getOrElse(false)
  val numIterations: Int = onumIterations.getOrElse(20)
  val lambda: Double = olambda.getOrElse(1.0)
  val rank: Int = orank.getOrElse(10)
  val numRecommends: Int = onumRecommends.getOrElse(20)
  val numUserBlocks: Int = onumUserBlocks.getOrElse(-1)
  val numProductBlocks: Int = onumProductBlocks.getOrElse(-1)
  val implicitPrefs: Boolean = oimplicitPrefs.getOrElse(false)
}


object AvlExample {


  def main(args: Array[String]) {
    val params = new Params(args)

    run(params)
  }

  private def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"ALS with $params")
    Common.setMaster(conf)
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))
        .set("spark.kryoserializer.buffer", "8m")
    }
    val sc = new SparkContext(conf)
    Common.setCheckPoint(sc)

    Logger.getRootLogger.setLevel(Level.WARN)

    val rawdata: RDD[SparseVector] = sc.objectFile(params.dataPath)

    val data: RDD[Rating] = Vector2Rating(rawdata)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numTraining = trainingData.count()
    val numTest = testData.count()
    println(s"Num of Training: $numTraining, Num of Test: $numTest.")

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(trainingData)

    val rmse = computeRmse(model, testData, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")

    // Recommend products for all users, enable the following code to test recommendForAll
    /*
    val userRecommend = model.recommendProductsForUsers(numRecommends)
    userRecommend.count()
    */
    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
  : Double = {

    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  def Vector2Rating(rawdata: RDD[SparseVector]): RDD[Rating] = {
    val Ratingdata: RDD[Rating] = rawdata.zipWithIndex().flatMap {
      case (v, i) =>
        val arr = mutable.ArrayBuilder.make[Rating]
        arr.sizeHint(v.numActives)
        v.foreachActive { (ii, vi) =>
          arr += Rating(i.toInt, ii, vi)
        }
        arr.result()
    }
    Ratingdata
  }

}

