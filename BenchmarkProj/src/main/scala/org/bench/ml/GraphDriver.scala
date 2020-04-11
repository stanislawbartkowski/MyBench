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
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf

/**
 * Compute NWeight for Graph G(V, E) as defined below.
 *
 * Weight(1)(u, v) = edge(u, v)
 * Weight(n)(u, v) =
 *   Sum (over {x|there are edges (u, x) and (x, v)}) Weight(n-1)(u, x) * Weight(1)(x, v)
 *
 * Input is given in Text file format. Each line represents a Node and all out edges of that node
 * (edge weight specified)
 * <vertex> <vertex1>:<weight1>,<vertex2>:<weight2> ...)
 */

// --dataPath input --output output --step 3 --maxDegree 30 --storageLevel 7 --model graphx

object NWeight extends Serializable{

  private class Params(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner("""
    GBT: an example of Gradient Boosted Tree for classification

    Example: spark-submit target/scala-2.11/benchmarkproj_2.11-0.1.jar  --dataPath <inputDir>

    For usage see below:
    """)
    private val odataPath = opt[String]("dataPath",required = true)
    private val ooutput = opt[String]("output", required = true)
    private val ostep = opt[Int]("step", required = true)
    private val omaxDegree = opt[Int]("maxDegree", required = true)
    private val ostorageLevel = opt[Int]("storageLevel", required = true)
    private val odisableKryo = opt[Boolean]("disableKryo", required = true)
    private val omodel = opt[String]("model", required = true)
    verify()
    val dataPath : String = odataPath.getOrElse("")
    val output  :String = ooutput.getOrElse("")
    val step: Int = ostep.getOrElse(-1)
    val maxDegree : Int = omaxDegree.getOrElse(-1)
    val storageLevel : StorageLevel = ostorageLevel.getOrElse(-1) match {
      case 0 => StorageLevel.OFF_HEAP
      case 1 => StorageLevel.DISK_ONLY
      case 2 => StorageLevel.DISK_ONLY_2
      case 3 => StorageLevel.MEMORY_ONLY
      case 4 => StorageLevel.MEMORY_ONLY_2
      case 5 => StorageLevel.MEMORY_ONLY_SER
      case 6 => StorageLevel.MEMORY_ONLY_SER_2
      case 7 => StorageLevel.MEMORY_AND_DISK
      case 8 => StorageLevel.MEMORY_AND_DISK_2
      case 9 => StorageLevel.MEMORY_AND_DISK_SER
      case 10 => StorageLevel.MEMORY_AND_DISK_SER_2
      case _ => StorageLevel.MEMORY_AND_DISK
    }
    val disableKryo :Boolean = odisableKryo.getOrElse(false)
    val model : String  = omodel.getOrElse("")
  }


  def parseArgs(args: Array[String]) = {
    val params = new Params(args)
    val input = params.dataPath
    val output =  params.output
    val step = params.step
    val maxDegree = params.maxDegree
    val storageLevel = params.storageLevel
    val disableKryo = params.disableKryo
    val model = params.model

    (input, output, step, maxDegree, storageLevel, disableKryo, model)
  }

  def main(args: Array[String]) {
    val (input, output, step, maxDegree, storageLevel, disableKryo, model) = parseArgs(args)

    if(!disableKryo) {
      System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }
    val sparkConf = new SparkConf()
    Common.setMaster(sparkConf)
    if (model.toLowerCase == "graphx")
      sparkConf.setAppName("NWeightGraphX")
    else
      sparkConf.setAppName("NWeightPregel")
    val sc = new SparkContext(sparkConf)
    Common.setCheckPoint(sc)
    val numPartitions = Common.getNumOfPartitons(sc)

    if (model.toLowerCase == "graphx") {
      GraphxNWeight.nweight(sc, input, output, step, maxDegree, numPartitions, storageLevel)
    } else {
      GraphPregelNWeight.nweight(sc, input, output, step, maxDegree, numPartitions, storageLevel)
    }

    sc.stop()
  }
}
