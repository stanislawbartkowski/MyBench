package org.bench.ml


import scala.collection.mutable

class LongDoubleMap {

  private val m: mutable.Map[Long, Double] =new mutable.HashMap[Long,Double]();

  def put(key : Long, value : Double) = m(key) = value

  def foreach[U](f: ((Long, Double)) => U): Unit = m.foreach(f)

  def get(key : Long) : Double = m(key)

  def size = m.size

}
