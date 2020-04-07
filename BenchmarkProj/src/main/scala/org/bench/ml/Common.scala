package org.bench.ml

import org.apache.spark.{SparkConf, SparkContext}

object Common {

  def getNumOfPartitons(sc : SparkContext): Int = {
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    parallel / 2
  }

  def setMaster(conf : SparkConf) = {
    conf.setMaster("local[2]")
  }

  def setCheckPoint(sc : SparkContext) = {
    sc.setCheckpointDir("checkpoint")
  }


}
