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
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
//import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap

/** * Compute NWeight for Graph G(V, E) as defined below *     Weight(1)(u, v) = edge(u, v)
 *     Weight(n)(u, v) = Sum (over {x|there are edges (u, x) and (x, v)}) Weight(n-1)(u, x)*Weight(1)(x, v)
 *
 * Input is given in Text file format. Each line represents a Node and all out edges of that node (edge weight specified)
 *  <vertex> <vertex1>:<weight1>,<vertex2>:<weight2> ...)
 */

object GraphxNWeight extends Serializable{

  def mapF(edge: EdgeContext[SizedPriorityQueue, Double, LongDoubleMap]) = {
    val theMap = new LongDoubleMap()
    val edgeAttribute = edge.attr
    val id = edge.srcId
    println("before mapF")
    edge.dstAttr.foreach{ case (target, wn) =>
      if (target != id)
        theMap.put(target, wn * edgeAttribute)
    }
    println("after mapF = " + theMap.size)
    edge.sendToSrc(theMap)
    println("after send = " + theMap.size)
  }

  def reduceF(c1: LongDoubleMap, c2: LongDoubleMap) = {
    println("before reduceF c2=" + c2.size)
    c2.foreach(pair => c1.put(pair._1, c1.get(pair._1) + pair._2))
    println("after foreach c1=" + c1.size)
    c1
  }

  def updateF(id: VertexId, vdata: SizedPriorityQueue, msg: Option[LongDoubleMap]) = {
    vdata.clear()
    val weightMap = msg.orNull
    if (weightMap != null) {
      weightMap.foreach { pair =>
        val src = pair._1
        val wn = pair._2
        vdata.enqueue((src, wn))
      }
    }
    vdata
  }

  def nweight(sc: SparkContext, input: String, output: String, step: Int,
              maxDegree: Int, numPartitions: Int, storageLevel: StorageLevel) {

    //val start1 = System.currentTimeMillis
    val part = new HashPartitioner(numPartitions)
    val r = sc.textFile(input)
    val l1 = r.take(1)
    val l2 = r.take(2)
    val edges = sc.textFile(input, numPartitions).flatMap { line =>
      val fields = line.split("\\s+", 2)
      val src = fields(0).trim.toLong

      fields(1).split("[,\\s]+").filter(_.isEmpty() == false).map { pairStr =>
        val pair = pairStr.split(":")
        val (dest, weight) = (pair(0).trim.toLong, pair(1).toDouble)
        (src, Edge(src, dest, weight))
      }
    }.partitionBy(part).map(_._2)


    println("0) ========================================================")
    println("edges:" + edges.count())
    println("0) ========================================================")
    val vertices = edges.map { e =>
      (e.srcId, (e.dstId, e.attr))
    }.groupByKey(part).map { case (id, seq) =>
      val vdata = new SizedPriorityQueue(maxDegree)
      seq.foreach(vdata.enqueue)
      (id, vdata)
    }

    println("00) ========================================================")
    var g = GraphImpl(vertices, edges, new SizedPriorityQueue(maxDegree), storageLevel, storageLevel).cache()

    var msg: RDD[(VertexId, LongDoubleMap)] = null
    for (i <- 2 to step) {
      msg = g.aggregateMessages(mapF,reduceF)
      println("msg =================" + msg.count())
      g = g.outerJoinVertices(msg)(updateF).persist(storageLevel)
    }
    println("11) ========================================================")
    println("edges"+ g.edges.count());
    println("111) ========================================================")
    println("vertices"+ g.vertices.count());
    println("2) ========================================================")

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
