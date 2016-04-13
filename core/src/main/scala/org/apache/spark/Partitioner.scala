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

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.util.hashing.byteswap32

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.util.random.{XORShiftRandom, SamplingUtils}

/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a partition ID, from 0 to `numPartitions - 1`.
 */
abstract class Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, the number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */
  def defaultPartitioner(rdd: RDD[_], others: RDD[_]*): Partitioner = {
    val bySize = (Seq(rdd) ++ others).sortBy(_.partitions.size).reverse
    for (r <- bySize if r.partitioner.isDefined && r.partitioner.get.numPartitions > 0) {
      return r.partitioner.get
    }
    if (rdd.context.conf.contains("spark.default.parallelism")) {
      new HashPartitioner(rdd.context.defaultParallelism)
    } else {
      new HashPartitioner(bySize.head.partitions.size)
    }
  }
}

/**
 * A [[org.apache.spark.Partitioner]] that implements hash-based partitioning using
 * Java's `Object.hashCode`.
 *
 * Java arrays have hashCodes that are based on the arrays' identities rather than their contents,
 * so attempting to partition an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 */
class HashPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

/**
 * A [[org.apache.spark.Partitioner]] that partitions sortable records by range into roughly
 * equal ranges. The ranges are determined by sampling the content of the RDD passed in.
 *
 * Note that the actual number of partitions created by the RangePartitioner might not be the same
 * as the `partitions` parameter, in the case where the number of sampled records is less than
 * the value of `partitions`.
 */
class RangePartitioner[K : Ordering : ClassTag, V](
    partitions: Int,
    rdd: RDD[_ <: Product2[K, V]],
    private var ascending: Boolean = true)
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  // An array of upper bounds for the first (partitions - 1) partitions
  private var rangeBounds: Array[K] = {
    if (partitions <= 1) {
      Array.empty
    } else {
      // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
      val sampleSize = math.min(20.0 * partitions, 1e6)
      // Assume the input partitions are roughly balanced and over-sample a little bit.
      val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.size).toInt
      val (numItems, sketched) = RangePartitioner.sketch(rdd.map(_._1), sampleSizePerPartition)
      if (numItems == 0L) {
        Array.empty
      } else {
        // If a partition contains much more than the average number of items, we re-sample from it
        // to ensure that enough items are collected from that partition.
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        val candidates = ArrayBuffer.empty[(K, Float)]
        val imbalancedPartitions = mutable.Set.empty[Int]
        sketched.foreach { case (idx, n, sample) =>
          if (fraction * n > sampleSizePerPartition) {
            imbalancedPartitions += idx
          } else {
            // The weight is 1 over the sampling probability.
            val weight = (n.toDouble / sample.size).toFloat
            for (key <- sample) {
              candidates += ((key, weight))
            }
          }
        }
        if (imbalancedPartitions.nonEmpty) {
          // Re-sample imbalanced partitions with the desired sampling probability.
          val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
          val seed = byteswap32(-rdd.id - 1)
          val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
          val weight = (1.0 / fraction).toFloat
          candidates ++= reSampled.map(x => (x, weight))
        }
        RangePartitioner.determineBounds(candidates, partitions)
      }
    }
  }

  def numPartitions: Int = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    if (ascending) {
      partition
    } else {
      rangeBounds.length - partition
    }
  }

  override def equals(other: Any): Boolean = other match {
    case r: RangePartitioner[_, _] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

private[spark] object RangePartitioner {

  /**
   * Sketches the input RDD via reservoir sampling on each partition.
   *
   * @param rdd the input RDD to sketch
   * @param sampleSizePerPartition max sample size per partition
   * @return (total number of items, an array of (partitionId, number of items, sample))
   */
  def sketch[K : ClassTag](
      rdd: RDD[K],
      sampleSizePerPartition: Int): (Long, Array[(Int, Long, Array[K])]) = {
    val shift = rdd.id
    // val classTagK = classTag[K] // to avoid serializing the entire partitioner object
    val sketched = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = SamplingUtils.reservoirSampleAndCount(
        iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }.collect()
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /**
   * Determines the bounds for range partitioning from candidates with weights indicating how many
   * items each represents. Usually this is 1 over the probability used to sample this candidate.
   *
   * @param candidates unordered candidates with weights
   * @param partitions number of partitions
   * @return selected bounds
   */
  def determineBounds[K : Ordering : ClassTag](
      candidates: ArrayBuffer[(K, Float)],
      partitions: Int): Array[K] = {
    val ordering = implicitly[Ordering[K]]
    val ordered = candidates.sortBy(_._1)
    val numCandidates = ordered.size
    val sumWeights = ordered.map(_._2.toDouble).sum
    val step = sumWeights / partitions
    var cumWeight = 0.0
    var target = step
    val bounds = ArrayBuffer.empty[K]
    var i = 0
    var j = 0
    var previousBound = Option.empty[K]
    while ((i < numCandidates) && (j < partitions - 1)) {
      val (key, weight) = ordered(i)
      cumWeight += weight
      if (cumWeight >= target) {
        // Skip duplicate values.
        if (previousBound.isEmpty || ordering.gt(key, previousBound.get)) {
          bounds += key
          target += step
          j += 1
          previousBound = Some(key)
        }
      }
      i += 1
    }
    bounds.toArray
  }
}

class AdaptivePartitioner(var numPartitions: Int,
    var boundsOpt: Option[Array[Double]],
    var centroidsOpt: Option[Array[(Double,Int)]],
    var intervalsOpt: Option[Array[(Double,Double)]]) extends Partitioner {

  import com.tdunning.math.stats.{TDigest, AVLTreeDigest}
  import java.nio.ByteBuffer
  import scala.collection.JavaConversions._
  import scala.collection.immutable.Vector
  import scala.collection.mutable.Map

  val nVirtuals = 2

  def this() = this(0, None, None, None)
  def this(numPartitions: Int) = this(numPartitions, None, None, None)

  def getPartition(key: Any): Int = boundsOpt match {
    //case Some(intervals) => getPartitionIntervals(key, intervals)
    //case Some(centroids) => getPartitionCentroids(key, centroids)
    case Some(bounds) => getPartitionBounds (key, bounds)
    case None => getPartitionHash (key)
  }

  private def getPartitionHash(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }

  private def getPartitionBounds(key: Any, bounds: Array[Double]): Int = {
    for (i <- 0 until bounds.size) {
      val k = key.hashCode
      if (k <= bounds(i)) {
        return i
      }
    }
    return bounds.size - 1 
  }
  
  private def getPartitionCentroids(key: Any, centroids: Array[(Double,Int)]): Int = {
    val k = key.hashCode
    for (i <- 0 until centroids.size) {
      val (centroid,partId) = centroids(i)
      if (k <= centroid) {
        if (i > 0 && (k - centroids(i-1)._1) < (centroid - k)) {
          return centroids(i-1)._2
        } else {
          return partId
        }
      }
    }
    return numPartitions - 1
  }

  def getPartitionIntervals(key: Any, intervals: Array[(Double,Double)]) = {
    @scala.annotation.tailrec
    def findPartition (intervals: Array[(Double,Double)], d: Int, from: Int, to: Int): Int = {
      if (from == to) from
      else {
        val middle = (to + from) / 2
        intervals(middle) match {
          case (lower,_) if lower > d =>
            findPartition (intervals, d, from, middle - 1)
          case (_,upper) if upper < d =>
            findPartition (intervals, d, middle + 1, to)
          case _ => middle
        }
      }
    }
    findPartition (intervals, key.hashCode, 0, intervals.size - 1)
  }

  def setTDigest_(tdigest: TDigest) = {

    println (s"tdigest size = ${tdigest.size}")

    val centroidsMap = scala.collection.mutable.Map.empty[Double,Int].withDefaultValue(0)
    tdigest.centroids.foreach (c => centroidsMap.update(c.mean, centroidsMap(c.mean) + c.count))
    val totalWeight = centroidsMap.values.sum
    val targetWeight = (totalWeight / numPartitions) max centroidsMap.values.max

    val centroids = centroidsMap.toArray
    println (s"targetWeight=${targetWeight})")
    println (s"centroids(totalWeight=${totalWeight}) \n ${centroids.mkString("\n")}")

    val centroidGroups = Map.empty[Int, (Vector[Double], Int)]
    for ((m,c) <- centroids.iterator) {
      var part = -1
      val ccIter = centroidGroups.iterator
      while (ccIter.hasNext && part == -1) {
        val (i, (centroids, weight)) = ccIter.next
        if (weight + c <= targetWeight)
          part = i
      }
      centroidGroups.get(part) match {
        case Some(ccGroup) =>
          centroidGroups.update (part, (ccGroup._1 :+ m, ccGroup._2 + c))
        case None =>
          centroidGroups.update (centroidGroups.size, (Vector(m), c))
      }
    }
    

    centroidsOpt = Some(
      centroidGroups.flatMap {case (part, (centroids, _)) => centroids.iterator zip Iterator.continually(part)}.toArray.sortBy(_._1)
    )
    
    println (s"centroids \n ${centroidsOpt.get.mkString("\n")}")

    numPartitions = centroidGroups.size
  }

  def setTDigest(tdigest: TDigest) = {
    println (s"tdigest size = ${tdigest.size}; total = ${tdigest.centroids.map(_.count).sum}; bytes = ${tdigest.byteSize}")
    //val centroidsMap = scala.collection.mutable.Map.empty[Double,Int].withDefaultValue(0)
    //tdigest.centroids.foreach (c => centroidsMap.update(c.mean, centroidsMap(c.mean) + c.count))
    def evaluateBounds(step: Double): Array[Double] = {

      var quantiles = (step to 1d by step).toArray
      quantiles(quantiles.size - 1) = 1d

      val bounds = quantiles.map (q => tdigest.quantile (q))

      val quantilesAndBounds = (quantiles zip bounds).zipWithIndex

      println (s"quantiles and bounds \n${quantilesAndBounds.mkString("\n")}")

      if (bounds.toSet.size == bounds.size) {
        bounds.toArray
      } else {
        var maxContiguous = Int.MinValue
        var currContiguous = 1
        for (i <- 1 until bounds.size) {
          if (bounds(i) == bounds(i-1)) {
            currContiguous += 1
            if (currContiguous > maxContiguous) {
              maxContiguous = currContiguous
            }
          } else {
            currContiguous = 1
          }
        }
        bounds.toArray
        evaluateBounds (step * maxContiguous)
      }
    }
    val step = 1d / numPartitions
    val bounds = evaluateBounds(step)
    //var quantiles = (step to 1d by step).toArray
    //quantiles(quantiles.size - 1) = 1d
    //val bounds = quantiles.map (q => tdigest.quantile (q))
    println (s"bounds \n ${bounds.mkString("\n")}")
    numPartitions = bounds.size
    boundsOpt = Some(bounds)
  }

  def setTDigest__(tdigest: TDigest) = {
    val step = 1d / numPartitions
    val thresholds = (0d to 1d by step).toArray

    val intervals = getIntervals (tdigest, thresholds)
    val bounds = intervals.
      map (d => (tdigest.quantile(d._1), tdigest.quantile(d._2))).
      unzip._2

    //intervalsOpt = Some(intervals)
    boundsOpt = Some(bounds.toArray)
    numPartitions = intervals.size
  }

  private def getIntervals(digest: TDigest, thresholds: Array[Double]): Array[(Double,Double)] = {
    def makeIntervs(thresholds: Array[Double]) = {
      val intervs = (thresholds.dropRight(1) zip thresholds.drop(1))
      intervs
    }

    if (thresholds.size <= 1) return Array.empty
    else if (thresholds.size == 2) return Array((thresholds(0),thresholds(1)))

    val (lower, upper) = (thresholds(0), thresholds(thresholds.size - 1))

    val bounds = thresholds.map (d => digest.quantile(d))
    val newThresholds = bounds.map (n => digest.cdf(n)).toSet.toArray.sorted
    if (newThresholds.size == thresholds.size)
      return makeIntervs (thresholds)

    newThresholds(0) = lower
    newThresholds(newThresholds.size - 1) = upper

    if (newThresholds.size == 1) return Array.empty

    val intervs = makeIntervs (newThresholds)

    val largerInterv = intervs.maxBy (interv => interv._2 - interv._1)

    val newStep = largerInterv._2 - largerInterv._1

    def split(lower: Double, upper: Double, step: Double): Array[(Double,Double)] = {
      val n = scala.math.ceil(((upper - lower) / step)).toInt
      val newStep = (upper - lower) / n
      val thresholds = (lower to upper by newStep).toArray
      thresholds(0) = lower
      thresholds(thresholds.size - 1) = upper
      getIntervals (digest, thresholds)
    }

    val splitLeftIntervs = split (lower, largerInterv._1, newStep)
    val splitRightIntervs = split (largerInterv._2, upper, newStep)

    val finalIntervs = splitLeftIntervs ++ List(largerInterv) ++ splitRightIntervs

    finalIntervs
  }


  override def equals(other: Any): Boolean = other match {
    case h: AdaptivePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions

  override def toString = s"AdaptivePartitioner(${numPartitions}, ${boundsOpt})"

}
