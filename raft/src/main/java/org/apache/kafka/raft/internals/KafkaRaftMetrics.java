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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.apache.kafka.raft.FollowerState;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.QuorumState;

import java.util.concurrent.TimeUnit;

public class KafkaRaftMetrics implements AutoCloseable {

    private final Metrics metrics;
    private final long bootTimestamp;

    private OffsetAndEpoch logEndOffset;
    private int numVoterConnections;
    private long electionStartMs;
    private long pollStartMs;
    private long pollEndMs;

    private final MetricName currentLeaderIdMetricName;
    private final MetricName currentVotedIdMetricName;
    private final MetricName currentEpochMetricName;
    private final MetricName currentStateMetricName;
    private final MetricName highWatermarkMetricName;
    private final MetricName logEndOffsetMetricName;
    private final MetricName logEndEpochMetricName;
    private final MetricName bootTimestampMetricName;
    private final MetricName numVoterConnectionsMetricName;
    private final Sensor commitTimeSensor;
    private final Sensor electionTimeSensor;
    private final Sensor fetchRecordsSensor;
    private final Sensor appendRecordsSensor;
    private final Sensor pollIdleSensor;

    public KafkaRaftMetrics(Metrics metrics, String metricGrpPrefix, QuorumState state, long timestamp) {
        this.metrics = metrics;
        this.bootTimestamp = timestamp;
        String metricGroupName = metricGrpPrefix + "-metrics";

        this.pollStartMs = 0L;
        this.pollEndMs = 0L;
        this.electionStartMs = -1L;
        this.numVoterConnections = 0;
        this.logEndOffset = new OffsetAndEpoch(0L, 0);

        this.currentStateMetricName = metrics.metricName("current-state", metricGroupName, "The current state of this member; possible values are leader, candidate, voter, observer");
        Gauge<String> stateProvider = (mConfig, currentTimeMs) -> {
            if (state.isLeader()) {
                return "leader";
            } else if (state.isCandidate()) {
                return "candidate";
            } else {
                if (state.isVoter())
                    return "voter";
                else
                    return "observer";
            }
        };
        metrics.addMetric(this.currentStateMetricName, null, stateProvider);

        this.currentLeaderIdMetricName = metrics.metricName("current-leader", metricGroupName, "The current quorum leader's id; -1 indicates unknown");
        metrics.addMetric(this.currentLeaderIdMetricName, (mConfig, currentTimeMs) -> state.leaderId().orElse(-1));

        this.currentVotedIdMetricName = metrics.metricName("current-vote", metricGroupName, "The current voted leader's id; -1 indicates not voted for anyone");
        metrics.addMetric(this.currentVotedIdMetricName, (mConfig, currentTimeMs) -> {
            if (state.isLeader() || state.isCandidate()) {
                return state.localId;
            } else {
                FollowerState followerState = state.followerStateOrThrow();
                if (followerState.hasVoted())
                    return followerState.votedId();
                else
                    return -1;
            }
        });

        this.currentEpochMetricName = metrics.metricName("current-epoch", metricGroupName, "The current quorum epoch.");
        metrics.addMetric(this.currentEpochMetricName, (mConfig, currentTimeMs) -> state.epoch());

        this.highWatermarkMetricName = metrics.metricName("high-watermark", metricGroupName, "The high watermark maintained on this member; -1 if it is unknown");
        metrics.addMetric(this.highWatermarkMetricName, (mConfig, currentTimeMs) -> state.highWatermark().orElse(-1L));

        this.logEndOffsetMetricName = metrics.metricName("log-end-offset", metricGroupName, "The current raft log end offset.");
        metrics.addMetric(this.logEndOffsetMetricName, (mConfig, currentTimeMs) -> logEndOffset.offset);

        this.logEndEpochMetricName = metrics.metricName("log-end-epoch", metricGroupName, "The current raft log end epoch.");
        metrics.addMetric(this.logEndEpochMetricName, (mConfig, currentTimeMs) -> logEndOffset.epoch);

        this.bootTimestampMetricName = metrics.metricName("boot-timestamp", metricGroupName, "The bootstrapped timestamp of this member.");
        metrics.addMetric(this.bootTimestampMetricName, (mConfig, currentTimeMs) -> bootTimestamp);

        this.numVoterConnectionsMetricName = metrics.metricName("number-unknown-voter-connections", metricGroupName, "The number of voter connections recognized at this member.");
        metrics.addMetric(this.numVoterConnectionsMetricName, (mConfig, currentTimeMs) -> numVoterConnections);

        this.pollIdleSensor = metrics.sensor("poll-idle-ratio");
        this.pollIdleSensor.add(metrics.metricName("poll-idle-ratio-avg",
                metricGroupName,
                "The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records."),
                new Avg());

        this.commitTimeSensor = metrics.sensor("commit-latency");
        this.commitTimeSensor.add(metrics.metricName("commit-latency-avg", metricGroupName,
                "The average time in milliseconds to commit an entry in the raft log."), new Avg());
        this.commitTimeSensor.add(metrics.metricName("commit-latency-max", metricGroupName,
                "The maximum time in milliseconds to commit an entry in the raft log."), new Max());

        this.electionTimeSensor = metrics.sensor("election-latency");
        this.electionTimeSensor.add(metrics.metricName("election-latency-avg", metricGroupName,
                "The average time in milliseconds to elect a new leader."), new Avg());
        this.electionTimeSensor.add(metrics.metricName("election-latency-max", metricGroupName,
                "The maximum time in milliseconds to elect a new leader."), new Max());

        this.fetchRecordsSensor = metrics.sensor("fetch-records");
        this.fetchRecordsSensor.add(metrics.metricName("fetch-records-rate", metricGroupName,
                "The average number of records fetched from the leader of the raft quorum."),
                new Rate(TimeUnit.SECONDS, new WindowedSum()));

        this.appendRecordsSensor = metrics.sensor("append-records");
        this.appendRecordsSensor.add(metrics.metricName("append-records-rate", metricGroupName,
                "The average number of records appended per sec as the leader of the raft quorum."),
                new Rate(TimeUnit.SECONDS, new WindowedSum()));
    }

    public void updatePollStart(long currentTimeMs) {
        if (pollEndMs != 0L) {
            long pollTimeMs = Math.max(pollEndMs - pollStartMs, 0L);
            long totalTimeMs = Math.max(currentTimeMs - pollStartMs, 1L);
            this.pollIdleSensor.record(pollTimeMs / (double) totalTimeMs, currentTimeMs);
        }

        this.pollStartMs = currentTimeMs;
    }

    public void updatePollEnd(long currentTimeMs) {
        this.pollEndMs = currentTimeMs;
    }

    public void updateLogEnd(OffsetAndEpoch logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public void updateNumUnknownVoterConnections(int numVoterConnections) {
        this.numVoterConnections = numVoterConnections;
    }

    public void updateAppendRecords(long numRecords) {
        appendRecordsSensor.record(numRecords);
    }

    public void updateFetchedRecords(long numRecords) {
        fetchRecordsSensor.record(numRecords);
    }

    public void updateCommitLatency(double latencyMs, long currentTimeMs) {
        commitTimeSensor.record(latencyMs, currentTimeMs);
    }

    public void updateElectionLatency(double latencyMs, long currentTimeMs) {
        electionTimeSensor.record(latencyMs, currentTimeMs);
    }

    public void updateElectionStartMs(long currentTimeMs) {
        electionStartMs = currentTimeMs;
    }

    public void maybeUpdateElectionLatency(long currentTimeMs) {
        if (electionStartMs > 0L) {
            electionTimeSensor.record(currentTimeMs - electionStartMs, currentTimeMs);
            electionStartMs = -1L;
        }
    }

    @Override
    public void close() {
        metrics.removeMetric(currentLeaderIdMetricName);
        metrics.removeMetric(currentVotedIdMetricName);
        metrics.removeMetric(currentEpochMetricName);
        metrics.removeMetric(currentStateMetricName);
        metrics.removeMetric(highWatermarkMetricName);
        metrics.removeMetric(logEndOffsetMetricName);
        metrics.removeMetric(logEndEpochMetricName);
        metrics.removeMetric(bootTimestampMetricName);
        metrics.removeMetric(numVoterConnectionsMetricName);

        metrics.removeSensor(commitTimeSensor.name());
        metrics.removeSensor(electionTimeSensor.name());
        metrics.removeSensor(fetchRecordsSensor.name());
        metrics.removeSensor(appendRecordsSensor.name());
        metrics.removeSensor(pollIdleSensor.name());
    }
}
