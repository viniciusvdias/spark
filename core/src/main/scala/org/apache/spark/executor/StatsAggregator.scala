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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.management.ManagementFactory
import java.net.URL
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.duration._

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.NonFatal

import com.tdunning.math.stats.{TDigest, AVLTreeDigest}

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.{AccumulableInfo, DirectTaskResult, IndirectTaskResult, Task, ShuffleMapTask, ResultTask, MapStatus}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._

import akka.actor.{Actor, ActorSystem, Inbox, Props}
import akka.util.Timeout

/** 
 * Akka messages
 */
case class TaskCheckIn[T <: Task[_]](task: T)
case class TaskCheckOut[T <: Task[_]](task: T, res: Any)
case class CheckOutAck(res: Any)

/**
 * Statistics aggregator
 */
class StatsAggregator extends Actor with Logging {

  val tdigestCompress = SparkEnv.get.conf.getInt ("spark.optimizer.tdigest.compress", 100)
  val runningPerStage = new HashMap[Int,(Int,TDigest)] // stageId -> (runningTasks,aggregatedDigest)
  var aggregated = 0

  logInfo (s"Started statsAggregator with tdigest compression = ${tdigestCompress}")

  def receive = {
    case TaskCheckIn(shuffleMapTask: ShuffleMapTask) =>
      handleShuffleMapTaskIn (shuffleMapTask)

    case TaskCheckOut(shuffleMapTask: ShuffleMapTask, mapStatus: MapStatus) =>
      handleShuffleMapTaskOut (shuffleMapTask, mapStatus)
      sender ! CheckOutAck(mapStatus)

    case (msg @ TaskCheckOut(_, res: Any)) =>
      //logWarning (s"Undefined message ${msg}")
      sender ! CheckOutAck(res)
  }

  private [this] def handleShuffleMapTaskIn(shuffleMapTask: ShuffleMapTask) =
    runningPerStage.get(shuffleMapTask.stageId) match {
      case Some( (runningTasks, tdigest) ) =>
        runningPerStage.update (shuffleMapTask.stageId, (runningTasks + 1, tdigest))
      case None =>
        runningPerStage.update (shuffleMapTask.stageId, (1,TDigest.createAvlTreeDigest(tdigestCompress)))
    }

  private [this] def handleShuffleMapTaskOut(shuffleMapTask: ShuffleMapTask, mapStatus: MapStatus) =
    runningPerStage.get(shuffleMapTask.stageId) match {
      
      case Some ( (runningTasks, _tdigest) ) if runningTasks == 1 => // last task, piggybacking
        val tdigest = mapStatus.getTDigest.get
        tdigest.add (_tdigest)
        aggregated += 1
        runningPerStage.remove (shuffleMapTask.stageId)
        
      case Some ( (runningTasks, tdigest) ) => // just do aggregation, unset mapStatus
        tdigest.add (mapStatus.getTDigest.get)
        mapStatus.unsetTDigest
        aggregated += 1
        runningPerStage.update (shuffleMapTask.stageId, (runningTasks - 1, tdigest) )

      case _ =>
        logWarning (s"If ${shuffleMapTask} is calling checkout must be an entry of its checkin")
    
    }

  override def postStop = {
    assert (runningPerStage.isEmpty)
  }
}


