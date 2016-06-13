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

package org.apache.spark

import scala.reflect.ClassTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ShuffleHandle

import org.apache.spark.scheduler.StageMetrics

/**
 * :: DeveloperApi ::
 * Base class for dependencies.
 */
@DeveloperApi
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
  def equiv(_that: Any): Boolean = _that match {
    case that: Dependency[_] =>
      this.rdd.equiv (that.rdd)
    case _ => false
  }
}


/**
 * :: DeveloperApi ::
 * Base class for dependencies where each partition of the child RDD depends on a small number
 * of partitions of the parent RDD. Narrow dependencies allow for pipelined execution.
 */
@DeveloperApi
abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}


/**
 * :: DeveloperApi ::
 * Represents a dependency on the output of a shuffle stage. Note that in the case of shuffle,
 * the RDD is transient since we don't need it on the executor side.
 *
 * @param _rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to None,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 * @param keyOrdering key ordering for RDD's shuffles
 * @param aggregator map/reduce-side aggregator for RDD's shuffle
 * @param mapSideCombine whether to perform partial aggregation (also known as map-side combine)
 */
@DeveloperApi
class ShuffleDependency[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient private val _rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Option[Serializer] = None,
    val keyOrdering: Option[Ordering[K]] = None,
    val aggregator: Option[Aggregator[K, V, C]] = None,
    val mapSideCombine: Boolean = false,
    val depOwnerOpt: Option[RDD[_]] = None)
  extends Dependency[Product2[K, V]] {

  override def rdd: RDD[Product2[K, V]] = _rdd.asInstanceOf[RDD[Product2[K, V]]]

  /** adaptive execution */
  def depOwner = depOwnerOpt.get

  var _updated = false

  def updated: Boolean = _updated

  def shouldInstrument: Boolean =
    partitioner.isInstanceOf[AdaptivePartitioner] && !updated

  def adapt(stats: MapOutputStatistics, metrics: StageMetrics): Option[Int] = partitioner match {
    case thisPartitioner: AdaptivePartitioner =>
      thisPartitioner.adapt (stats, metrics)
      _rdd.context.env.shuffleManager.unregisterShuffle (shuffleId)
      val oldShuffleId = shuffleId
      shuffleId = _rdd.context.newShuffleId()
      shuffleHandle = newShuffleHandle
      _updated = true
      Some(oldShuffleId)
    case _ =>
      None
  }

  def adaptFromShuffleDep(other: ShuffleDependency[_,_,_]): Option[Int] = (partitioner, other.partitioner) match {
    case (thisPartitioner: AdaptivePartitioner, thatPartitioner: AdaptivePartitioner) =>
      thisPartitioner.adaptFromPartitioner (thatPartitioner)
      _rdd.context.env.shuffleManager.unregisterShuffle (shuffleId)
      val oldShuffleId = shuffleId
      shuffleId = _rdd.context.newShuffleId()
      shuffleHandle = newShuffleHandle
      _updated = true
      Some(oldShuffleId)
    case _ =>
      None
  }

  def scale(parentMetrics: Seq[StageMetrics]): Option[Int] = partitioner match {
    case thisPartitioner: AdaptivePartitioner =>
      val updated = thisPartitioner.scale (parentMetrics)
      if (updated) {
        val oldShuffleId = shuffleId
        shuffleId = _rdd.context.newShuffleId()
        shuffleHandle = newShuffleHandle
        Some(oldShuffleId)
      } else {
        None
      }
    case _ =>
      None
  }

  private def newShuffleHandle = {
    _rdd.context.env.shuffleManager.
      registerShuffle(shuffleId, _rdd.partitions.size, this)
  }
  /*****/

  private[spark] val keyClassName: String = reflect.classTag[K].runtimeClass.getName
  private[spark] val valueClassName: String = reflect.classTag[V].runtimeClass.getName
  // Note: It's possible that the combiner class tag is null, if the combineByKey
  // methods in PairRDDFunctions are used instead of combineByKeyWithClassTag.
  private[spark] val combinerClassName: Option[String] =
    Option(reflect.classTag[C]).map(_.runtimeClass.getName)

  var shuffleId: Int = _rdd.context.newShuffleId()

  var shuffleHandle: ShuffleHandle = newShuffleHandle

  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))

  override def toString = {
    s"ShuffleDep(shuffleId=${shuffleId}, rdd=${rdd}, partitioner=${partitioner}, updated=${updated})"
  }

}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
@DeveloperApi
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}


/**
 * :: DeveloperApi ::
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
@DeveloperApi
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int): List[Int] = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
