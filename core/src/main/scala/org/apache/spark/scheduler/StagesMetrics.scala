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
  private[spark] def setShuffleRecordsWritten(value: Long) = _shuffleRecordsWritten = value
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.io.{IOException, ObjectInputStream}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.DataReadMethod.DataReadMethod
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util.{Utils, SizeEstimator, CallSite}

import org.apache.spark.executor.TaskMetrics

import org.apache.spark.{Logging, SparkException}

import scala.collection.mutable.HashMap

/**
 * :: DeveloperApi ::
 * Metrics aggregated from task metrics of this stage.
 */
@DeveloperApi
class StagesMetrics extends SparkListener with Logging {

  private val stagesMetrics: HashMap[Int,Option[StageMetrics]] = new HashMap()
  
  /** client functions */

  def numTrackedStages = stagesMetrics.size
  def trackedStages = stagesMetrics.values

  def trackStage(stage: Stage) = {
    stagesMetrics.update (stage.id, None)
    stage match {
      case shufStage: ShuffleMapStage =>
        logWarning (s"shufStage added: ${shufStage}, ${shufStage.callSite.shortForm} (shuffleId=${shufStage.shuffleDep.shuffleId})")
      case resStage: ResultStage =>
        logWarning (s"resStage added (${resStage}: ${resStage.callSite.shortForm})")
    }
  }

  def untrackStage(stageId: Int): Unit = {
    val stageOpt = stagesMetrics.remove (stageId)
    logWarning (s"Untracking stageId: ${stageId}: ${stageOpt}")
  }
  
  def lastEquiv(stage: Stage): StageMetrics = {
    val candidates = stagesMetrics.filter {
      case (_,Some(sm)) if sm.name == stage.name => true
      case _ => false
    }.toSeq.sortBy (_._1)

    for (i <- 0 until candidates.size - 1) {
      val stageId = candidates(i)._1
      untrackStage (stageId)
    }
    val (stageId,lastEquiv) = candidates(candidates.size - 1)
    logWarning (s"Keeping stageId: ${stageId}")
    lastEquiv.get
  }

  def trackingStage(stage: Stage) = stagesMetrics contains stage.id

  def get(stageId: Int): StageMetrics = stagesMetrics.get(stageId) match {
    case Some(Some(sm)) => synchronized {
      val start = System.nanoTime
      while (!sm.finished) wait
      val end = System.nanoTime
      //logWarning (s"func waited ${end - start} ns")
      assert (sm.finished)
      sm
    }

    case Some(None) =>
      throw new SparkException (s"Metrics for stage ${stageId} are not ready")

    case None =>
      throw new SparkException (s"Stage ${stageId} not being tracked by StagesMetrics")
  }

  def get(callSite: CallSite): StageMetrics = get(findStageId(callSite))

  private def findStageId(callSite: CallSite): Int = {
    val filtered = stagesMetrics.find {
      case (stageId,Some(metrics)) => metrics.name == callSite.shortForm
      case _ => false
    }
    logWarning (s"callSite=${callSite.shortForm}; filtered = ${filtered}")
    logWarning (s"${this}")
    assert (filtered.size == 1)
    filtered.head._1
  }

  /** listener functions */
  
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
    stagesMetrics.get(stageSubmitted.stageInfo.stageId) match {
   
    case Some(None) =>
      val sm = new StageMetrics(stageSubmitted.stageInfo)
      stagesMetrics.update (stageSubmitted.stageInfo.stageId, Some(sm))

    case Some(Some(sm)) =>
      // assert completed
      assert (sm.finished)

    case None => // not tracking this stage
  }
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = {
  }


  // TODO: take into account task end reason and failures
  //case class SparkListenerTaskEnd(
  //  stageId: Int,
  //  stageAttemptId: Int,
  //  taskType: String,
  //  reason: TaskEndReason,
  //  taskInfo: TaskInfo,
  //  taskMetrics: TaskMetrics)
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) =
    stagesMetrics.get(taskEnd.stageId) match {

    case Some(Some(sm)) =>
      sm.aggregate (taskEnd.taskInfo.index, taskEnd.taskMetrics)
      if (sm.finished) synchronized {
          notifyAll
      }

    case None => // not tracking this stage

    case _ =>
      throw new SparkException("")
  }

  override def toString = {
    s"StagesMetrics(${stagesMetrics.mkString("; ")})"
  }
}

@DeveloperApi
class StageMetrics(stageInfo: StageInfo) {

  def stageId = stageInfo.stageId
  def numTasks = stageInfo.numTasks
  def name = stageInfo.name

  private var aggregated = 0

  private var tasksMetrics: Array[TaskMetrics] = new Array(numTasks)

  /* task statistics */

  // common
  private var _sumRuntimes: Long = 0L
  def avgRuntime: Double = _sumRuntimes / numTasks.toDouble

  // spill
  private var _spill: Long = 0L
  def spill: Long = _spill
  
  // input
  private var _inputRecordsRead: Long = 0L
  def inputRecordsRead: Long = _inputRecordsRead

  // shuffle read
  private var _shuffleRecordsRead: Long = 0L
  def shuffleRecordsRead: Long = _shuffleRecordsRead

  def recordsRead: Long = shuffleRecordsRead + inputRecordsRead

  // shuffle write
  private var _shuffleBytesWritten: Long = 0L
  def shuffleBytesWritten: Long = _shuffleBytesWritten
  
  private var _shuffleRecordsWritten: Long = 0L
  def shuffleRecordsWritten: Long = _shuffleRecordsWritten

  def recordsWritten: Long = _shuffleRecordsWritten

  // garbage collection
  private var _maxGcTime: Long = Long.MinValue
  def maxGcTime: Long = _maxGcTime

  private var _minGcTime: Long = Long.MaxValue
  def minGcTime: Long = _minGcTime

  /**
   * Aggregates new task metrics into these stage metrics
   */
  def aggregate(taskIndex: Int, tm: TaskMetrics) = {
    assert (tasksMetrics(taskIndex) == null)
    // total executor runtime
    _sumRuntimes += tm.executorRunTime

    // gargbage collection
    _maxGcTime = _maxGcTime max tm.jvmGCTime
    _minGcTime = _minGcTime min tm.jvmGCTime

    // aggregated spill (memory + disk)
    _spill += tm.memoryBytesSpilled + tm.diskBytesSpilled

    // input metrics
    if (tm.inputMetrics.isDefined) {
      val metrics = tm.inputMetrics.get
      _inputRecordsRead += metrics.recordsRead
    }

    // has shuffle read
    if (tm.shuffleReadMetrics.isDefined) {
      val metrics = tm.shuffleReadMetrics.get
      _shuffleRecordsRead += metrics.recordsRead
    }

    // has shuffle write (shuffle map stage)
    if (tm.shuffleWriteMetrics.isDefined) {
      val metrics = tm.shuffleWriteMetrics.get
      _shuffleBytesWritten += metrics.shuffleBytesWritten
      _shuffleRecordsWritten += metrics.shuffleRecordsWritten
    }

    tasksMetrics(taskIndex) = tm
    aggregated += 1
  }

  def finished: Boolean = aggregated == numTasks

  override def toString = {
    s"StageMetrics(stageId=${stageId}, numTasks=${numTasks}, aggregated=${aggregated}" +
      s", name=${name}, sizeInBytes=${SizeEstimator.estimate(this)}" +
      //s", avgRuntime=${avgRuntime}" +
      //s", shuffleBytesWritten=${shuffleBytesWritten}" +
      //s", spill=${spill}" +
      //s", maxGcTime=${maxGcTime}" +
      //s", minGcTime=${minGcTime}" +
      //s", inputRecordsRead=${inputRecordsRead}" +
      //s", shuffleRecordsRead=${shuffleRecordsRead}" +
      //s", recordsRead=${recordsRead}" +
      ")"
  }
      

}
