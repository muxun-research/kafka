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
package org.apache.kafka.coordinator.group.generic;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.coordinator.group.generic.GenericGroupState.*;
import static org.junit.jupiter.api.Assertions.*;

public class GenericGroupTest {
    private final String protocolType = "consumer";
    private final String groupInstanceId = "groupInstanceId";
    private final String memberId = "memberId";
    private final String clientId = "clientId";
    private final String clientHost = "clientHost";
    private final int rebalanceTimeoutMs = 60000;
    private final int sessionTimeoutMs = 10000;

    private GenericGroup group = null;

    @BeforeEach
    public void initialize() {
        group = new GenericGroup(new LogContext(), "groupId", EMPTY, Time.SYSTEM);
    }

    @Test
    public void testCanRebalanceWhenStable() {
        assertTrue(group.canRebalance());
    }

    @Test
    public void testCanRebalanceWhenCompletingRebalance() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        assertTrue(group.canRebalance());
    }

    @Test
    public void testCannotRebalanceWhenPreparingRebalance() {
        group.transitionTo(PREPARING_REBALANCE);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testCannotRebalanceWhenDead() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        group.transitionTo(DEAD);
        assertFalse(group.canRebalance());
    }

    @Test
    public void testStableToPreparingRebalanceTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertState(group, PREPARING_REBALANCE);
    }

    @Test
    public void testStableToDeadTransition() {
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testAwaitingRebalanceToPreparingRebalanceTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(PREPARING_REBALANCE);
        assertState(group, PREPARING_REBALANCE);
    }

    @Test
    public void testPreparingRebalanceToDeadTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testPreparingRebalanceToEmptyTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        assertState(group, EMPTY);
    }

    @Test
    public void testEmptyToDeadTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(EMPTY);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testAwaitingRebalanceToStableTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);
        assertState(group, STABLE);
    }

    @Test
    public void testEmptyToStableIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testStableToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        group.transitionTo(STABLE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testEmptyToAwaitingRebalanceIllegalTransition() {
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PREPARING_REBALANCE));
    }

    @Test
    public void testPreparingRebalanceToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testAwaitingRebalanceToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(COMPLETING_REBALANCE);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testDeadToDeadIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        group.transitionTo(DEAD);
        assertState(group, DEAD);
    }

    @Test
    public void testDeadToStableIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(STABLE));
    }

    @Test
    public void testDeadToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(PREPARING_REBALANCE));
    }

    @Test
    public void testDeadToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PREPARING_REBALANCE);
        group.transitionTo(DEAD);
        assertThrows(IllegalStateException.class, () -> group.transitionTo(COMPLETING_REBALANCE));
    }

    @Test
    public void testSelectProtocol() {
        List<Protocol> member1Protocols = Arrays.asList(new Protocol("range", new byte[0]), new Protocol("roundrobin", new byte[0]));

        GenericGroupMember member1 = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member1Protocols);
        group.add(member1);

        List<Protocol> member2Protocols = Arrays.asList(new Protocol("roundrobin", new byte[0]), new Protocol("range", new byte[0]));

        GenericGroupMember member2 = new GenericGroupMember("member2", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member2Protocols);
        group.add(member2);

        // now could be either range or robin since there is no majority preference
        assertTrue(group.selectProtocol().equals("range") || group.selectProtocol().equals("roundrobin"));

        GenericGroupMember member3 = new GenericGroupMember("member3", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member2Protocols);
        group.add(member3);

        // now we should prefer 'roundrobin'
        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test
    public void testSelectProtocolRaisesIfNoMembers() {
        assertThrows(IllegalStateException.class, () -> group.selectProtocol());
    }

    @Test
    public void testSelectProtocolChoosesCompatibleProtocol() {
        List<Protocol> member1Protocols = Arrays.asList(new Protocol("range", new byte[0]), new Protocol("roundrobin", new byte[0]));

        GenericGroupMember member1 = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member1Protocols);
        group.add(member1);

        List<Protocol> member2Protocols = Arrays.asList(new Protocol("roundrobin", new byte[0]), new Protocol("blah", new byte[0]));

        GenericGroupMember member2 = new GenericGroupMember("member2", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member2Protocols);
        group.add(member2);

        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test
    public void testSupportsProtocols() {
        List<Protocol> member1Protocols = Arrays.asList(new Protocol("range", new byte[0]), new Protocol("roundrobin", new byte[0]));

        GenericGroupMember member1 = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, member1Protocols);

        // by default, the group supports everything
        assertTrue(group.supportsProtocols(protocolType, mkSet("range", "roundrobin")));

        group.add(member1);
        group.transitionTo(PREPARING_REBALANCE);

        assertTrue(group.supportsProtocols(protocolType, mkSet("roundrobin", "foo")));
        assertTrue(group.supportsProtocols(protocolType, mkSet("range", "bar")));
        assertFalse(group.supportsProtocols(protocolType, mkSet("foo", "bar")));
    }

    @Test
    public void testSubscribedTopics() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("range", ConsumerProtocol.serializeSubscription(new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))).array())));

        group.transitionTo(PREPARING_REBALANCE);
        group.add(member);

        group.initNextGeneration();

        Set<String> expectedTopics = new HashSet<>(Collections.singleton("foo"));
        assertEquals(expectedTopics, group.subscribedTopics().get());

        group.transitionTo(PREPARING_REBALANCE);
        group.remove(memberId);

        group.initNextGeneration();

        assertEquals(Optional.of(Collections.emptySet()), group.subscribedTopics());

        GenericGroupMember memberWithFaultyProtocol = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("range", new byte[0])));

        group.transitionTo(PREPARING_REBALANCE);
        group.add(memberWithFaultyProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testSubscribedTopicsNonConsumerGroup() {
        // not able to compute it for a newly created group
        assertEquals(Optional.empty(), group.subscribedTopics());

        GenericGroupMember memberWithNonConsumerProtocol = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, "My Protocol", Collections.singletonList(new Protocol("range", new byte[0])));

        group.transitionTo(PREPARING_REBALANCE);
        group.add(memberWithNonConsumerProtocol);

        group.initNextGeneration();

        assertEquals(Optional.empty(), group.subscribedTopics());
    }

    @Test
    public void testInitNextGeneration() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.transitionTo(PREPARING_REBALANCE);
        group.add(member, new CompletableFuture<>());

        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertEquals("roundrobin", group.protocolName().orElse(null));
    }

    @Test
    public void testInitNextGenerationEmptyGroup() {
        assertEquals(EMPTY, group.currentState());
        assertEquals(0, group.generationId());
        assertNull(group.protocolName().orElse(null));

        group.transitionTo(PREPARING_REBALANCE);
        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertNull(group.protocolName().orElse(null));
    }

    @Test
    public void testUpdateMember() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);

        List<Protocol> newProtocols = Arrays.asList(new Protocol("range", new byte[0]), new Protocol("roundrobin", new byte[0]));
        int newRebalanceTimeoutMs = 120000;
        int newSessionTimeoutMs = 20000;
        group.updateMember(member, newProtocols, newRebalanceTimeoutMs, newSessionTimeoutMs, null);

        assertEquals(group.rebalanceTimeoutMs(), newRebalanceTimeoutMs);
        assertEquals(member.sessionTimeoutMs(), newSessionTimeoutMs);
        assertEquals(newProtocols, member.supportedProtocols());
    }

    @Test
    public void testReplaceGroupInstanceWithNonExistingMember() {
        String newMemberId = "newMemberId";
        assertThrows(IllegalArgumentException.class, () -> group.replaceStaticMember(groupInstanceId, memberId, newMemberId));
    }

    @Test
    public void testReplaceGroupInstance() throws Exception {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.of(groupInstanceId), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        group.add(member, joinGroupFuture);

        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        member.setAwaitingSyncFuture(syncGroupFuture);

        assertTrue(group.isLeader(memberId));
        assertEquals(memberId, group.staticMemberId(groupInstanceId));

        String newMemberId = "newMemberId";
        group.replaceStaticMember(groupInstanceId, memberId, newMemberId);

        assertTrue(group.isLeader(newMemberId));
        assertEquals(newMemberId, group.staticMemberId(groupInstanceId));
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), joinGroupFuture.get().errorCode());
        assertEquals(Errors.FENCED_INSTANCE_ID.code(), syncGroupFuture.get().errorCode());
        assertFalse(member.isAwaitingJoin());
        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCompleteJoinFuture() throws Exception {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        CompletableFuture<JoinGroupResponseData> joinGroupFuture = new CompletableFuture<>();
        group.add(member, joinGroupFuture);

        assertTrue(group.hasAllMembersJoined());
        assertTrue(group.completeJoinFuture(member, new JoinGroupResponseData().setMemberId(member.memberId()).setErrorCode(Errors.NONE.code())));

        assertEquals(Errors.NONE.code(), joinGroupFuture.get().errorCode());
        assertEquals(memberId, joinGroupFuture.get().memberId());
        assertFalse(member.isAwaitingJoin());
        assertEquals(0, group.numAwaitingJoinResponse());
    }

    @Test
    public void testNotCompleteJoinFuture() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);

        assertFalse(member.isAwaitingJoin());
        assertFalse(group.completeJoinFuture(member, new JoinGroupResponseData().setMemberId(member.memberId()).setErrorCode(Errors.NONE.code())));

        assertFalse(member.isAwaitingJoin());
    }

    @Test
    public void testCompleteSyncFuture() throws Exception {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        CompletableFuture<SyncGroupResponseData> syncGroupFuture = new CompletableFuture<>();
        member.setAwaitingSyncFuture(syncGroupFuture);

        assertTrue(group.completeSyncFuture(member, new SyncGroupResponseData().setErrorCode(Errors.NONE.code())));

        assertEquals(0, group.numAwaitingJoinResponse());

        assertFalse(member.isAwaitingSync());
        assertEquals(Errors.NONE.code(), syncGroupFuture.get().errorCode());
    }

    @Test
    public void testNotCompleteSyncFuture() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertFalse(member.isAwaitingSync());

        assertFalse(group.completeSyncFuture(member, new SyncGroupResponseData().setErrorCode(Errors.NONE.code())));

        assertFalse(member.isAwaitingSync());
    }

    @Test
    public void testCannotAddPendingMemberIfStable() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertThrows(IllegalStateException.class, () -> group.addPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingAfterMemberIsStable() {
        group.addPendingMember(memberId);
        assertFalse(group.hasMemberId(memberId));
        assertTrue(group.isPendingMember(memberId));

        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertTrue(group.hasMemberId(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testRemovalFromPendingWhenMemberIsRemoved() {
        group.addPendingMember(memberId);
        assertFalse(group.hasMemberId(memberId));
        assertTrue(group.isPendingMember(memberId));

        group.remove(memberId);
        assertFalse(group.hasMemberId(memberId));
        assertFalse(group.isPendingMember(memberId));
    }

    @Test
    public void testCannotAddStaticMemberIfAlreadyPresent() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.of(groupInstanceId), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertTrue(group.hasMemberId(memberId));
        assertTrue(group.hasStaticMember(groupInstanceId));

        // We are not permitted to add the member again if it is already present
        assertThrows(IllegalStateException.class, () -> group.add(member));
    }

    @Test
    public void testCannotAddPendingSyncOfUnknownMember() {
        assertThrows(IllegalStateException.class, () -> group.addPendingSyncMember(memberId));
    }

    @Test
    public void testCannotRemovePendingSyncOfUnknownMember() {
        assertThrows(IllegalStateException.class, () -> group.removePendingSyncMember(memberId));
    }

    @Test
    public void testCanAddAndRemovePendingSyncMember() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.removePendingSyncMember(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testRemovalFromPendingSyncWhenMemberIsRemoved() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.of(groupInstanceId), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.remove(memberId);
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testNewGenerationClearsPendingSyncMembers() {
        GenericGroupMember member = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(member);
        group.transitionTo(PREPARING_REBALANCE);
        assertTrue(group.addPendingSyncMember(memberId));
        assertEquals(Collections.singleton(memberId), group.allPendingSyncMembers());
        group.initNextGeneration();
        assertEquals(Collections.emptySet(), group.allPendingSyncMembers());
    }

    @Test
    public void testElectNewJoinedLeader() {
        GenericGroupMember leader = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(leader);
        assertTrue(group.isLeader(memberId));
        assertFalse(leader.isAwaitingJoin());

        GenericGroupMember newLeader = new GenericGroupMember("new-leader", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));
        group.add(newLeader, new CompletableFuture<>());

        GenericGroupMember newMember = new GenericGroupMember("new-member", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));
        group.add(newMember);

        assertTrue(group.maybeElectNewJoinedLeader());
        assertTrue(group.isLeader("new-leader"));
    }

    @Test
    public void testMaybeElectNewJoinedLeaderChooseExisting() {
        GenericGroupMember leader = new GenericGroupMember(memberId, Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));

        group.add(leader, new CompletableFuture<>());
        assertTrue(group.isLeader(memberId));
        assertTrue(leader.isAwaitingJoin());

        GenericGroupMember newMember = new GenericGroupMember("new-member", Optional.empty(), clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, Collections.singletonList(new Protocol("roundrobin", new byte[0])));
        group.add(newMember);

        assertTrue(group.maybeElectNewJoinedLeader());
        assertTrue(group.isLeader(memberId));
    }

    private void assertState(GenericGroup group, GenericGroupState targetState) {
        Set<GenericGroupState> otherStates = new HashSet<>();
        otherStates.add(STABLE);
        otherStates.add(PREPARING_REBALANCE);
        otherStates.add(COMPLETING_REBALANCE);
        otherStates.add(DEAD);
        otherStates.remove(targetState);

        otherStates.forEach(otherState -> assertFalse(group.isInState(otherState)));
        assertTrue(group.isInState(targetState));
    }
}
