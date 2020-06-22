/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.raft

import java.util.{Objects, Optional, OptionalLong}

import kafka.log.{AppendOrigin, Log}
import kafka.server.{FetchHighWatermark, FetchLogEnd}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.raft
import org.apache.kafka.raft.{LogAppendInfo, LogFetchInfo, LogOffsetMetadata, OffsetMetadata, ReplicatedLog}

import scala.compat.java8.OptionConverters._

class KafkaMetadataLog(time: Time, log: Log, maxFetchSizeInBytes: Int = 1024 * 1024) extends ReplicatedLog {

  case class KafkaLogOffsetMetadata(segmentBaseOffset: Long, relativePositionInSegment: Int) extends OffsetMetadata {
    override def hashCode: Int = Objects.hash(segmentBaseOffset, relativePositionInSegment)
    override def equals(obj: Any): Boolean = obj match {
      case other: KafkaLogOffsetMetadata => segmentBaseOffset == other.segmentBaseOffset && relativePositionInSegment == other.relativePositionInSegment
      case _ => false
    }
    override def toString: String = s"(segmentBaseOffset=$segmentBaseOffset,relativePositionInSegment=$relativePositionInSegment)"
  }

  override def read(startOffset: Long, endOffsetExclusive: OptionalLong): LogFetchInfo = {
    val isolation = if (endOffsetExclusive.isPresent)
      FetchHighWatermark
    else
      FetchLogEnd

    val fetchInfo = log.read(startOffset,
      maxLength = maxFetchSizeInBytes,
      isolation = isolation,
      minOneMessage = true)

    new LogFetchInfo(
      fetchInfo.records,

      new LogOffsetMetadata(
        fetchInfo.fetchOffsetMetadata.messageOffset,
        Optional.of(KafkaLogOffsetMetadata(
          fetchInfo.fetchOffsetMetadata.segmentBaseOffset,
          fetchInfo.fetchOffsetMetadata.relativePositionInSegment))
        )
    )
  }

  override def appendAsLeader(records: Records, epoch: Int): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    val appendInfo = log.appendAsLeader(records.asInstanceOf[MemoryRecords],
      leaderEpoch = epoch,
      origin = AppendOrigin.Coordinator)
    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def appendAsFollower(records: Records): LogAppendInfo = {
    if (records.sizeInBytes == 0)
      throw new IllegalArgumentException("Attempt to append an empty record set")

    val appendInfo = log.appendAsFollower(records.asInstanceOf[MemoryRecords])
    new LogAppendInfo(appendInfo.firstOffset.getOrElse {
      throw new KafkaException("Append failed unexpectedly")
    }, appendInfo.lastOffset)
  }

  override def lastFetchedEpoch: Int = {
    log.latestEpoch.getOrElse(0)
  }

  override def endOffsetForEpoch(leaderEpoch: Int): Optional[raft.OffsetAndEpoch] = {
    // TODO: Does this handle empty log case (when epoch is None) as we expect?
    val endOffsetOpt = log.endOffsetForEpoch(leaderEpoch).map { offsetAndEpoch =>
      new raft.OffsetAndEpoch(offsetAndEpoch.offset, offsetAndEpoch.leaderEpoch)
    }
    endOffsetOpt.asJava
  }

  override def endOffset: LogOffsetMetadata = {
    val endOffsetMetadata = log.logEndOffsetMetadata
    new LogOffsetMetadata(
      endOffsetMetadata.messageOffset,
      Optional.of(KafkaLogOffsetMetadata(
        endOffsetMetadata.segmentBaseOffset,
        endOffsetMetadata.relativePositionInSegment))
      )
  }

  override def startOffset: Long = {
    log.logStartOffset
  }

  override def truncateTo(offset: Long): Unit = {
    log.truncateTo(offset)
  }

  override def assignEpochStartOffset(epoch: Int, startOffset: Long): Unit = {
    log.maybeAssignEpochStartOffset(epoch, startOffset)
  }

  override def updateHighWatermark(offsetMetadata: LogOffsetMetadata): Unit = {
    if (offsetMetadata.metadata.isPresent && offsetMetadata.metadata.get.isInstanceOf[KafkaLogOffsetMetadata]) {
      val logOffsetMetadata = offsetMetadata.metadata.get.asInstanceOf[KafkaLogOffsetMetadata]
      log.updateHighWatermarkOffsetMetadata(new kafka.server.LogOffsetMetadata(
        offsetMetadata.offset,
        logOffsetMetadata.segmentBaseOffset,
        logOffsetMetadata.relativePositionInSegment)
      )
    } else {
      log.updateHighWatermark(offsetMetadata.offset)
    }
  }
}
