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


import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
// import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

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

object GraphPregelNWeight extends Serializable{

  def sendMsg(edge: EdgeTriplet[SizedPriorityQueue, Double]) = {
    val m = new LongDoubleMap()
    val w1 = edge.attr
    val id = edge.srcId
    edge.dstAttr.foreach{ case (target, wn) =>
      if (target != id)
        m.put(target, wn*w1)
    }
    Iterator((id, m))
  }

  def mergMsg(c1: LongDoubleMap, c2: LongDoubleMap) = {
    c2.foreach(pair =>
        c1.put(pair._1, c1.get(pair._1) + pair._2))
    c1
  }

  def vProg(id: VertexId, vdata: SizedPriorityQueue, msg: LongDoubleMap) = {
    vdata.clear()
    if (msg.size > 0) {
      msg.foreach { pair =>
        val src = pair._1
        val wn = pair._2
        vdata.enqueue((src, wn))
      }
      vdata
    } else {
      vdata.enqueue((id, 1))
      vdata
    }
  }

  def nweight(sc: SparkContext, input: String, output: String, step: Int,
              maxDegree: Int, numPartitions: Int, storageLevel: StorageLevel) {

    //val start1 = System.currentTimeMillis
    val part = new HashPartitioner(numPartitions)
    val edges = sc.textFile(input, numPartitions).flatMap { line =>
      val fields = line.split("\\s+", 2)
      val src = fields(0).trim.toLong

      fields(1).split("[,\\s]+").filter(_.isEmpty() == false).map { pairStr =>
        val pair = pairStr.split(":")
        val (dest, weight) = (pair(0).trim.toLong, pair(1).toDouble)
        (src, Edge(src, dest, weight))
      }
    }.partitionBy(part).map(_._2)

    var g = GraphImpl(edges, new SizedPriorityQueue(maxDegree), storageLevel, storageLevel).cache()

    g = Pregel(g, new LongDoubleMap, step, EdgeDirection.In)(
      vProg, sendMsg, mergMsg)

    g.vertices.map { case (vid, vdata) =>
      var s = new StringBuilder
      s.append(vid)

      vdata.foreach { r =>
        s.append(' ')
        s.append(r._1)
        s.append(':')
        s.append(r._2)
      }
      s.toString
    }.saveAsTextFile(output)
  }
}
