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


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.rogach.scallop.ScallopConf


object SVDExample {

  class Params(arguments: Seq[String]) extends ScallopConf(arguments) {

    // --dataPath input --numUsers 100 --numProducts 100 --sparsity 0.05

    banner(
      """
    Rating Data Generator example

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath", required = true)
    private val onumFeatures = opt[Int]("numFeatures", required = true)
    private val onumSingularValues = opt[Int]("numSingularValues", required = true)
    private val ocomputeU = opt[Boolean]("computeU", required = true)
    private val omaxResultSize = opt[String]("maxResultSize", required = true)
    verify()

    val dataPath: String = odataPath.getOrElse("")
    val numFeatures: Int = onumFeatures.getOrElse(0)
    val numSingularValues: Int = onumSingularValues.getOrElse(0)
    val computeU: Boolean = ocomputeU.getOrElse(true)
    val maxResultSize: String = omaxResultSize.getOrElse("1g")
  }


  def main(args: Array[String]): Unit = {
    val params = new Params(args)
    run(params)
  }

  def run(params: Params): Unit = {

    val conf = new SparkConf()
      .setAppName(s"SVD with $params")
      .set("spark.driver.maxResultSize", params.maxResultSize)
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numFeatures = params.numFeatures
    val numSingularValues = params.numSingularValues
    val computeU = params.computeU

    val data: RDD[Vector] = sc.objectFile(dataPath)
    val mat: RowMatrix = new RowMatrix(data)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(numSingularValues, computeU)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.

    sc.stop()
  }
}
