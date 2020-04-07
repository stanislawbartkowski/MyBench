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

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.{HashMap => MHashMap}
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

/**
 * :: DeveloperApi ::
 * Generate test data for LDA (Latent Dirichlet Allocation).
 * Each document is an example with a docId and a vector.
 * The vector is of length vocabulary size and each entry
 * represents the number of the corresponding word appearing.
 */
object LDADataGenerator {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numDocs 2000 --numVocab 1000 --docLenMin 50 --docLenMax 100000

    banner(
      """
    LDADataGenerator example

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val onumDocs = opt[Long]("numDocs", required = true)
    private val onumVocab = opt[Int]("numVocab", required = true)
    private val odocLenMin = opt[Int]("docLenMin", required = true)
    private val odocLenMax = opt[Int]("docLenMax", required = true)
    verify

    var dataPath = odataPath.getOrElse("")
    var numDocs: Long = onumDocs.getOrElse(500L)
    var numVocab: Int = onumVocab.getOrElse(1000)
    var docLenMin: Int = odocLenMin.getOrElse(50)
    var docLenMax: Int = odocLenMax.getOrElse(10000)
  }


  /**
   * Generate an RDD containing test data for LDA.
   *
   * @param sc        SparkContext to use for creating the RDD.
   * @param numDocs   Number of documents that will be contained in the RDD.
   * @param numVocab  Vocabulary size to generate for each document.
   * @param docLenMin Minimum document length.
   * @param docLenMax Maximum document length.
   * @param numParts  Number of partitions of the generated RDD. Default value is 3.
   * @param seed      Random seed for each partition.
   */
  def generateLDARDD(
                      sc: SparkContext,
                      numDocs: Long,
                      numVocab: Int,
                      docLenMin: Int,
                      docLenMax: Int,
                      numParts: Int = 3,
                      seed: Long = System.currentTimeMillis()): RDD[(Long, Vector)] = {
    val data = sc.parallelize(0L until numDocs, numParts).mapPartitionsWithIndex {
      (idx, part) =>
        val rng = new Random(seed ^ idx)
        part.map { case docIndex =>
          var currentSize = 0
          val entries = MHashMap[Int, Int]()
          val docLength = rng.nextInt(docLenMax - docLenMin + 1) + docLenMin
          while (currentSize < docLength) {
            val index = rng.nextInt(numVocab)
            entries(index) = entries.getOrElse(index, 0) + 1
            currentSize += 1
          }

          val iter = entries.toSeq.map(v => (v._1, v._2.toDouble))
          (docIndex, Vectors.sparse(numVocab, iter))
        }
    }
    data
  }

  def main(args: Array[String]) {

    val params = new Params(args)
    val conf = new SparkConf().setAppName("LDADataGenerator")
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    val numPartitions = Common.getNumOfPartitons(sc)


    var outputPath = params.dataPath
    var numDocs: Long = params.numDocs
    var numVocab: Int = params.numVocab
    var docLenMin: Int = params.docLenMin
    var docLenMax: Int = params.docLenMax
    val data = generateLDARDD(sc, numDocs, numVocab, docLenMin, docLenMax, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
