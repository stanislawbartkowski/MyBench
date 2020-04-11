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

import java.io._

import org.apache.spark.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object NWeightDataGenerator {

  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""
    GBT: an example of Gradient Boosted Tree for classification

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val omodelPath = opt[String]("modelPath",required = true)
    private val ototalNumRecords = opt[Long]("totalNumRecords",required = true)
    verify()
    val dataPath : String = odataPath.getOrElse("")
    var modelPath = omodelPath.getOrElse("")
    var totalNumRecords: Long = ototalNumRecords.getOrElse(0)
  }

  val MAX_ID: Int = 2401080

  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    val conf = new SparkConf().setAppName("NWeight-DataGeneration")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    var modelPath = params.modelPath
    var outputPath = params.dataPath
    var totalNumRecords: Long = params.totalNumRecords
    val numPartitions = Common.getNumOfPartitons(sc)
    val model: MatrixFactorizationModel = loadModel(modelPath, sc, numPartitions)

    var randomPairCount: Long = 0
    var randomPair: RDD[(Int, Int)] = null
    while (randomPairCount < totalNumRecords) {
      val numRecords = ((totalNumRecords - randomPairCount) * 1.2).toLong
      val numPerPartition = numRecords / numPartitions

      val seedRDD = sc.parallelize(1 to numPartitions, numPartitions)

      val randomPairOneTurn = seedRDD.flatMap { r =>
        val randNumbers = new ArrayBuffer[(Int, Int)]

        while (randNumbers.size < numPerPartition) {
          val id1 = Random.nextInt(MAX_ID)
          val id2 = Random.nextInt(MAX_ID)

          if (id1 != id2) {
            randNumbers += ((id1, id2))
          }
        }
        randNumbers
      }.distinct()

      if (randomPair == null) {
        randomPair = randomPairOneTurn
      } else {
        randomPair = randomPair.union(randomPairOneTurn)
      }
      randomPairCount = randomPair.count()
    }

    randomPair = randomPair.zipWithIndex()
      .filter(_._2 < totalNumRecords)
      .map(_._1)
      .sortBy(x => x)

    val predictRatings = model.predict(randomPair)

    val resultData = predictRatings.flatMap {
      case Rating(id1, id2, weight) =>
        Seq((id1, (id2, weight)), (id2, (id1, weight)))
    }.groupByKey().map { case (id1, iter) =>
      s"$id1\t" +
        iter.map { case (id, w) => f"$id:$w%.4f" }.mkString(",")
    }

    resultData.saveAsTextFile(outputPath)

    sc.stop()
  }

  def loadModel(modelPath: String, sc: SparkContext, partitions: Int): MatrixFactorizationModel = {
    val modelFile = new File(modelPath)

    if (modelFile.exists) {
      val in = new DataInputStream(new FileInputStream(modelPath))
      val weights = new Array[(Int, Double)](MAX_ID)
      for (i <- weights.indices) {
        val w = in.readFloat()
        weights(i) = (i, w)
      }
      in.close()

      val userFeatures = sc.parallelize(weights, math.max(400, partitions)).map { case (i, w) =>
        (i, Array(w))
      }
      val userFeaturesPartitioned = userFeatures.partitionBy(new HashPartitioner(partitions))
      userFeaturesPartitioned.cache()
      // Model matrix is symmetric, so productFeatures is the same with userFeatures
      new MatrixFactorizationModel(1, userFeaturesPartitioned, userFeaturesPartitioned)
    } else {
      throw new FileNotFoundException("No model file found in path: " + modelPath)
    }
  }
}
