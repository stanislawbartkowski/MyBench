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
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf

object LDAExample {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
   LDA: an example app for LDA.

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val ooutputPath = opt[String]("outputPath", required = true)
    private val ooptomizer = opt[String]("optimizer", required = true)
    private val omaxResultSize = opt[String]("maxResultSize", required = true)
    private val onumTopics = opt[Int]("numTopics", required = true)
    private val omaxIterations = opt[Int]("maxIterations", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
    val numTopics: Int = onumTopics.getOrElse(10)
    val maxIterations: Int = omaxIterations.getOrElse(10)
    val optimizer: String = ooptomizer.getOrElse("online")
    val maxResultSize: String = omaxResultSize.getOrElse("1g")
    val outputPath: String = ooutputPath.getOrElse("")
  }


  def main(args: Array[String]): Unit = {

    val params = new Params(args)
    run(params)
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf()
      .setAppName(s"LDA Example with $params")
      .set("spark.driver.maxResultSize", params.maxResultSize)
    Common.setMaster(conf)
    val sc = new SparkContext(conf)

    val corpus: RDD[(Long, Vector)] = sc.objectFile(params.dataPath)

    // Cluster the documents into numTopics topics using LDA
    val ldaModel = new LDA().setK(params.numTopics).setMaxIterations(params.maxIterations).setOptimizer(params.optimizer).run(corpus)

    // Save and load model.
    ldaModel.save(sc, params.outputPath)
    val savedModel = LocalLDAModel.load(sc, params.outputPath)

    sc.stop()
  }
}