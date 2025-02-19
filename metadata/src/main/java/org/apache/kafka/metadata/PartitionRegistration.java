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

package org.apache.kafka.metadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;


public class PartitionRegistration {

    /**
     * A builder class which creates a PartitionRegistration.
     */
    static public class Builder {
        private int[] replicas;
        private int[] isr;
        private int[] removingReplicas = Replicas.NONE;
        private int[] addingReplicas = Replicas.NONE;
        private Integer leader;
        private LeaderRecoveryState leaderRecoveryState;
        private Integer leaderEpoch;
        private Integer partitionEpoch;

        public Builder setReplicas(int[] replicas) {
            this.replicas = replicas;
            return this;
        }

        public Builder setIsr(int[] isr) {
            this.isr = isr;
            return this;
        }

        public Builder setRemovingReplicas(int[] removingReplicas) {
            this.removingReplicas = removingReplicas;
            return this;
        }

        public Builder setAddingReplicas(int[] addingReplicas) {
            this.addingReplicas = addingReplicas;
            return this;
        }

        public Builder setLeader(Integer leader) {
            this.leader = leader;
            return this;
        }

        public Builder setLeaderRecoveryState(LeaderRecoveryState leaderRecoveryState) {
            this.leaderRecoveryState = leaderRecoveryState;
            return this;
        }

        public Builder setLeaderEpoch(Integer leaderEpoch) {
            this.leaderEpoch = leaderEpoch;
            return this;
        }

        public Builder setPartitionEpoch(Integer partitionEpoch) {
            this.partitionEpoch = partitionEpoch;
            return this;
        }

        public PartitionRegistration build() {
            if (replicas == null) {
                throw new IllegalStateException("You must set replicas.");
            } else if (isr == null) {
                throw new IllegalStateException("You must set isr.");
            } else if (removingReplicas == null) {
                throw new IllegalStateException("You must set removing replicas.");
            } else if (addingReplicas == null) {
                throw new IllegalStateException("You must set adding replicas.");
            } else if (leader == null) {
                throw new IllegalStateException("You must set leader.");
            } else if (leaderRecoveryState == null) {
                throw new IllegalStateException("You must set leader recovery state.");
            } else if (leaderEpoch == null) {
                throw new IllegalStateException("You must set leader epoch.");
            } else if (partitionEpoch == null) {
                throw new IllegalStateException("You must set partition epoch.");
            }

            return new PartitionRegistration(replicas, isr, removingReplicas, addingReplicas, leader, leaderRecoveryState, leaderEpoch, partitionEpoch);
        }
    }

    public final int[] replicas;
    public final int[] isr;
    public final int[] removingReplicas;
    public final int[] addingReplicas;
    public final int leader;
    public final LeaderRecoveryState leaderRecoveryState;
    public final int leaderEpoch;
    public final int partitionEpoch;

    public static boolean electionWasClean(int newLeader, int[] isr) {
        return newLeader == NO_LEADER || Replicas.contains(isr, newLeader);
    }

    public PartitionRegistration(PartitionRecord record) {
        this(Replicas.toArray(record.replicas()), Replicas.toArray(record.isr()), Replicas.toArray(record.removingReplicas()), Replicas.toArray(record.addingReplicas()), record.leader(), LeaderRecoveryState.of(record.leaderRecoveryState()), record.leaderEpoch(), record.partitionEpoch());
    }

    private PartitionRegistration(int[] replicas, int[] isr, int[] removingReplicas, int[] addingReplicas, int leader, LeaderRecoveryState leaderRecoveryState, int leaderEpoch, int partitionEpoch) {
        this.replicas = replicas;
        this.isr = isr;
        this.removingReplicas = removingReplicas;
        this.addingReplicas = addingReplicas;
        this.leader = leader;
        this.leaderRecoveryState = leaderRecoveryState;
        this.leaderEpoch = leaderEpoch;
        this.partitionEpoch = partitionEpoch;
    }

    public PartitionRegistration merge(PartitionChangeRecord record) {
        int[] newReplicas = (record.replicas() == null) ? replicas : Replicas.toArray(record.replicas());
        int[] newIsr = (record.isr() == null) ? isr : Replicas.toArray(record.isr());
        int[] newRemovingReplicas = (record.removingReplicas() == null) ? removingReplicas : Replicas.toArray(record.removingReplicas());
        int[] newAddingReplicas = (record.addingReplicas() == null) ? addingReplicas : Replicas.toArray(record.addingReplicas());

        int newLeader;
        int newLeaderEpoch;
        if (record.leader() == NO_LEADER_CHANGE) {
            newLeader = leader;
            newLeaderEpoch = leaderEpoch;
        } else {
            newLeader = record.leader();
            newLeaderEpoch = leaderEpoch + 1;
        }

        LeaderRecoveryState newLeaderRecoveryState = leaderRecoveryState.changeTo(record.leaderRecoveryState());

        return new PartitionRegistration(newReplicas, newIsr, newRemovingReplicas, newAddingReplicas, newLeader, newLeaderRecoveryState, newLeaderEpoch, partitionEpoch + 1);
    }

    public String diff(PartitionRegistration prev) {
        StringBuilder builder = new StringBuilder();
        String prefix = "";
        if (!Arrays.equals(replicas, prev.replicas)) {
            builder.append(prefix).append("replicas: ").append(Arrays.toString(prev.replicas)).append(" -> ").append(Arrays.toString(replicas));
            prefix = ", ";
        }
        if (!Arrays.equals(isr, prev.isr)) {
            builder.append(prefix).append("isr: ").append(Arrays.toString(prev.isr)).append(" -> ").append(Arrays.toString(isr));
            prefix = ", ";
        }
        if (!Arrays.equals(removingReplicas, prev.removingReplicas)) {
            builder.append(prefix).append("removingReplicas: ").append(Arrays.toString(prev.removingReplicas)).append(" -> ").append(Arrays.toString(removingReplicas));
            prefix = ", ";
        }
        if (!Arrays.equals(addingReplicas, prev.addingReplicas)) {
            builder.append(prefix).append("addingReplicas: ").append(Arrays.toString(prev.addingReplicas)).append(" -> ").append(Arrays.toString(addingReplicas));
            prefix = ", ";
        }
        if (leader != prev.leader) {
            builder.append(prefix).append("leader: ").append(prev.leader).append(" -> ").append(leader);
            prefix = ", ";
        }
        if (leaderRecoveryState != prev.leaderRecoveryState) {
            builder.append(prefix).append("leaderRecoveryState: ").append(prev.leaderRecoveryState).append(" -> ").append(leaderRecoveryState);
            prefix = ", ";
        }
        if (leaderEpoch != prev.leaderEpoch) {
            builder.append(prefix).append("leaderEpoch: ").append(prev.leaderEpoch).append(" -> ").append(leaderEpoch);
            prefix = ", ";
        }
        if (partitionEpoch != prev.partitionEpoch) {
            builder.append(prefix).append("partitionEpoch: ").append(prev.partitionEpoch).append(" -> ").append(partitionEpoch);
        }
        return builder.toString();
    }

    public void maybeLogPartitionChange(Logger log, String description, PartitionRegistration prev) {
        if (!electionWasClean(leader, prev.isr)) {
            log.info("UNCLEAN partition change for {}: {}", description, diff(prev));
        } else if (log.isDebugEnabled()) {
            log.debug("partition change for {}: {}", description, diff(prev));
        }
    }

    public boolean hasLeader() {
        return leader != LeaderConstants.NO_LEADER;
    }

    public boolean hasPreferredLeader() {
        return leader == preferredReplica();
    }

    public int preferredReplica() {
        return replicas.length == 0 ? LeaderConstants.NO_LEADER : replicas[0];
    }

    public ApiMessageAndVersion toRecord(Uuid topicId, int partitionId) {
        return new ApiMessageAndVersion(new PartitionRecord().setPartitionId(partitionId).setTopicId(topicId).setReplicas(Replicas.toList(replicas)).setIsr(Replicas.toList(isr)).setRemovingReplicas(Replicas.toList(removingReplicas)).setAddingReplicas(Replicas.toList(addingReplicas)).setLeader(leader).setLeaderRecoveryState(leaderRecoveryState.value()).setLeaderEpoch(leaderEpoch).setPartitionEpoch(partitionEpoch), (short) 0);
    }

    public LeaderAndIsrPartitionState toLeaderAndIsrPartitionState(TopicPartition tp, boolean isNew) {
        return new LeaderAndIsrPartitionState().setTopicName(tp.topic()).setPartitionIndex(tp.partition()).setControllerEpoch(-1).setLeader(leader).setLeaderEpoch(leaderEpoch).setIsr(Replicas.toList(isr)).setPartitionEpoch(partitionEpoch).setReplicas(Replicas.toList(replicas)).setAddingReplicas(Replicas.toList(addingReplicas)).setRemovingReplicas(Replicas.toList(removingReplicas)).setLeaderRecoveryState(leaderRecoveryState.value()).setIsNew(isNew);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(replicas), Arrays.hashCode(isr), Arrays.hashCode(removingReplicas), Arrays.hashCode(addingReplicas), leader, leaderRecoveryState, leaderEpoch, partitionEpoch);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PartitionRegistration))
            return false;
        PartitionRegistration other = (PartitionRegistration) o;
        return Arrays.equals(replicas, other.replicas) && Arrays.equals(isr, other.isr) && Arrays.equals(removingReplicas, other.removingReplicas) && Arrays.equals(addingReplicas, other.addingReplicas) && leader == other.leader && leaderRecoveryState == other.leaderRecoveryState && leaderEpoch == other.leaderEpoch && partitionEpoch == other.partitionEpoch;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("PartitionRegistration(");
        builder.append("replicas=").append(Arrays.toString(replicas));
        builder.append(", isr=").append(Arrays.toString(isr));
        builder.append(", removingReplicas=").append(Arrays.toString(removingReplicas));
        builder.append(", addingReplicas=").append(Arrays.toString(addingReplicas));
        builder.append(", leader=").append(leader);
        builder.append(", leaderRecoveryState=").append(leaderRecoveryState);
        builder.append(", leaderEpoch=").append(leaderEpoch);
        builder.append(", partitionEpoch=").append(partitionEpoch);
        builder.append(")");
        return builder.toString();
    }

    public boolean hasSameAssignment(PartitionRegistration registration) {
        return Arrays.equals(this.replicas, registration.replicas) && Arrays.equals(this.addingReplicas, registration.addingReplicas) && Arrays.equals(this.removingReplicas, registration.removingReplicas);
    }
}
