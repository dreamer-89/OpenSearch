/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.cluster.routing.ShardRoutingState.STARTED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationAllocationIT extends SegmentReplicationBaseIT {

    private void createIndex(String idxName, int shardCount, int replicaCount, boolean isSegRep) {
        Settings.Builder builder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount);
        if (isSegRep) {
            builder = builder.put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT);
        } else {
            builder = builder.put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT);
        }
        prepareCreate(idxName, builder).get();
    }

    /**
     * This test verifies primary shard allocation is balanced.
     */
    public void testShardAllocation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 2;
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(maxReplicaCount + 1, 10);
        final int numberOfIndices = randomIntBetween(5, 10);

        final List<String> nodeNames = new ArrayList<>();
        logger.info("--> Creating {} nodes", nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().put(BalancedShardsAllocator.PRIMARY_SHARD_BALANCE_FACTOR_SETTING.getKey(), "1.0f")
                )
        );

        int shardCount, replicaCount, totalShardCount = 0, totalReplicaCount = 0;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            shardCount = randomIntBetween(1, maxShardCount);
            totalShardCount += shardCount;
            replicaCount = randomIntBetween(0, maxReplicaCount);
            totalReplicaCount += replicaCount;
            createIndex("test" + i, shardCount, replicaCount, i % 2 == 0);
            logger.info("--> Creating index {} with shard count {} and replica count {}", "test" + i, shardCount, replicaCount);
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
            state = client().admin().cluster().prepareState().execute().actionGet().getState();
            shardAllocations.printShardDistribution(state);
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        RoutingNodes nodes = state.getRoutingNodes();
        final float avgNumShards = (float) (totalShardCount) / (float) (nodes.size());
        final int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - 1.0f)));
        final int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + 1.0f)));

        for (RoutingNode node : nodes) {
            assertTrue(node.primaryShardsWithState(STARTED).size() >= minAvgNumberOfShards);
            assertTrue(node.primaryShardsWithState(STARTED).size() <= maxAvgNumberOfShards);
        }
    }

    /**
     * This test verifies shard allocation with changes to cluster config i.e. node add, removal keeps the primary shard
     * allocation balanced.
     */
    public void testAllocationWithDisruption() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 2;
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(maxReplicaCount + 1, 10);
        final int numberOfIndices = randomIntBetween(1, 10);

        logger.info("--> Creating {} nodes", nodeCount);
        final List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(BalancedShardsAllocator.PRIMARY_SHARD_BALANCE_FACTOR_SETTING.getKey(), "1.0f")
                        .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0f")
                        .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0f")
                        .build()
                )
        );

        int shardCount, replicaCount, totalShardCount = 0, totalReplicaCount = 0;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            shardCount = randomIntBetween(1, maxShardCount);
            totalShardCount += shardCount;
            replicaCount = randomIntBetween(1, maxReplicaCount);
            totalReplicaCount += replicaCount;
            logger.info("--> Creating index test{} with primary {} and replica {}", i, shardCount, replicaCount);
            createIndex("test" + i, shardCount, replicaCount, i % 2 == 0);
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
            if (logger.isTraceEnabled()) {
                state = client().admin().cluster().prepareState().execute().actionGet().getState();
                shardAllocations.printShardDistribution(state);
            }
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        float avgNumShards = (float) (totalShardCount) / (float) (state.getRoutingNodes().size());
        int minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - 1.0f)));
        int maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + 1.0f)));

        for (RoutingNode node : state.getRoutingNodes()) {
            assertTrue(node.primaryShardsWithState(STARTED).size() >= minAvgNumberOfShards);
            assertTrue(node.primaryShardsWithState(STARTED).size() <= maxAvgNumberOfShards);
        }

        final int additionalNodeCount = randomIntBetween(1, 5);
        logger.info("--> Adding {} nodes", additionalNodeCount);

        internalCluster().startNodes(additionalNodeCount);
        assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        avgNumShards = (float) (totalShardCount) / (float) (state.getRoutingNodes().size());
        minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - 1.0f)));
        maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + 1.0f)));
        shardAllocations.printShardDistribution(state);
        for (RoutingNode node : state.getRoutingNodes()) {
            assertTrue(node.primaryShardsWithState(STARTED).size() >= minAvgNumberOfShards);
            assertTrue(node.primaryShardsWithState(STARTED).size() <= maxAvgNumberOfShards);
        }

        logger.info("--> Stop one third nodes");
        for (int i = 1; i < nodeCount; i += 3) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(i)));
            // give replica a chance to promote as primary before terminating node containing the replica
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        avgNumShards = (float) (totalShardCount) / (float) (state.getRoutingNodes().size());
        minAvgNumberOfShards = Math.round(Math.round(Math.floor(avgNumShards - 1.0f)));
        maxAvgNumberOfShards = Math.round(Math.round(Math.ceil(avgNumShards + 1.0f)));
        shardAllocations.printShardDistribution(state);

        for (RoutingNode node : state.getRoutingNodes()) {
            assertTrue(node.primaryShardsWithState(STARTED).size() >= minAvgNumberOfShards);
            assertTrue(node.primaryShardsWithState(STARTED).size() <= maxAvgNumberOfShards);
        }
    }

    /**
     * This test verifies shard allocation results in even enabled primary shards specifically for segrep enabled shards.
     * It stops a set of nodes and ensure the unassigned segrep shards are assigned evenly on active nodes.
     *
     * Step 1. Create 4 nodes with 4 index (2 docrep, 2 segrep) with 2 shards and 0 replica. Each node should have 1 docrep, 1 segrep index. N1(1, 1), N2(1,1), N3(1,1), N4(1,1)
     * Step 2. Stop 2 nodes, this should result in 4 shard unassignment, 2 docrep, 2 segrep. N1(1,1), N2(1,1).
     * Step 3. Add 2 nodes back, this should assign the shards back to nodes, this has chances of final allocation with. N1(1,1), N2(1,1), N3(0,2), N4(2,0).   (1,3)
     *
     */
    public void testPrimaryBalanceForSegRepEnabled() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int nodeCount = 4;
        final List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }

        createIndex("a", 4, 0, true);
        createIndex("b", 4, 0, false);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("c")
                .setSettings(
                    Settings.builder()
                        .put("index.unassigned.node_left.delayed_timeout", "100ms")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                )
                .get()
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("d")
                .setSettings(
                    Settings.builder()
                        .put("index.unassigned.node_left.delayed_timeout", "100ms")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                )
                .get()
        );

        final Set<String> segrepShardSet = new HashSet<>();
        segrepShardSet.add("a");
        segrepShardSet.add("c");
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertTrue(state.getRoutingNodes().size() == 4);

        logger.info("--> Stop node 3 and node 4");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(2)));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(3)));
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertTrue(state.getRoutingNodes().size() == 2);

        logger.info("--> Add 2 nodes back");
        internalCluster().startNode();
        internalCluster().startNode();

        logger.info("--> Cluster State --> {}", client().admin().cluster().prepareState().execute().actionGet().getState());

        // The test times out if there are no replicas
        assertBusy(() -> ensureYellow(), 120, TimeUnit.SECONDS);
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        assertTrue(state.getRoutingNodes().size() == 4);

        ShardAllocations shardAllocations = new ShardAllocations();
        final int totalPrimaryShardCount = 16;
        float avgNumShardsSR = (float) (totalPrimaryShardCount) / (float) (state.getRoutingNodes().size());
        float minAvgNumberOfShardsSR = Math.round(Math.round(Math.floor(avgNumShardsSR - 1.0f)));
        float maxAvgNumberOfShardsSR = Math.round(Math.round(Math.ceil(avgNumShardsSR + 1.0f)));

        logger.info("--> Verify shard allocation with totalShards {} noOfNodes {} minAvg {} and maxAvg {} ", totalPrimaryShardCount, state.getRoutingNodes().size(), minAvgNumberOfShardsSR, maxAvgNumberOfShardsSR);
        shardAllocations.printShardDistribution(state);
//        for(ObjectObjectCursor<String, IndexMetadata> indexMetaData: state.metadata().indices().valuesIt()) {
//            if (indexMetaData.value.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE).equals(ReplicationType.SEGMENT)) {
//                totalShardsCounterSR.updateAndGet(v -> new Float((float) (v + indexMetaData.value)))
//            }
//        }
        for (RoutingNode node : state.getRoutingNodes()) {
            List<ShardRouting> startedShards = node.primaryShardsWithState(STARTED);
            int segRepShardCount = 0;
            for(ShardRouting routing: startedShards) {
                if (segrepShardSet.contains(routing.shardId().getIndexName())) {
                    segRepShardCount++;
                }
            }
            assertTrue(segRepShardCount >= minAvgNumberOfShardsSR);
            assertTrue(segRepShardCount <= maxAvgNumberOfShardsSR);
        }

    }

    public void testPrimaryBalanceForSegRepEnabled2() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final int maxReplicaCount = 2;
        final int maxShardCount = 5;
        final int nodeCount = randomIntBetween(maxReplicaCount + 1, 10);
        final int numberOfIndices = randomIntBetween(1, 10);

        final Set<String> segrepShardSet = new HashSet<>();
        logger.info("--> Creating {} nodes", nodeCount);
        final List<String> nodeNames = new ArrayList<>();
        for (int i = 0; i < nodeCount; i++) {
            nodeNames.add(internalCluster().startNode());
        }

        int shardCount, replicaCount, totalShardCount = 0, totalReplicaCount = 0, totalPrimaryShardCount = 0;
        ShardAllocations shardAllocations = new ShardAllocations();
        ClusterState state;
        for (int i = 0; i < numberOfIndices; i++) {
            shardCount = randomIntBetween(1, maxShardCount);
            totalShardCount += shardCount;
            replicaCount = randomIntBetween(1, maxReplicaCount);
            totalReplicaCount += replicaCount;
            String indexName = "test" + i;
            logger.info("--> Creating index {} with primary {} and replica {}", indexName, shardCount, replicaCount);
            boolean segrepIndex = randomBoolean();
            createIndex(indexName, shardCount, replicaCount, segrepIndex);
            if (segrepIndex) {
                segrepShardSet.add(indexName);
                totalPrimaryShardCount+= shardCount;
            }
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
            if (logger.isTraceEnabled()) {
                state = client().admin().cluster().prepareState().execute().actionGet().getState();
                shardAllocations.printShardDistribution(state);
            }
        }

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(BalancedShardsAllocator.PRIMARY_SHARD_BALANCE_FACTOR_SETTING.getKey(), "1.0f")
                        .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), "0.0f")
                        .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), "0.0f")
                        .build()
                )
        );
        logger.info("--> Stop one third nodes");
        for (int i = 1; i < nodeCount; i += 3) {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeNames.get(i)));
            // give replica a chance to promote as primary before terminating the node containing the replica
            assertBusy(() -> ensureGreen(), 60, TimeUnit.SECONDS);
        }
        state = client().admin().cluster().prepareState().execute().actionGet().getState();

//        AtomicInteger totalShardsCounter = new AtomicInteger();
//        state.metadata().indices().valuesIt().forEachRemaining(indexMetadata -> {
//            if (indexMetadata.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE).equals(ReplicationType.SEGMENT)) {
//                totalShardsCounter.addAndGet(indexMetadata.getNumberOfShards());
//            }
//        });
        float avgNumShardsSR = (float) (totalPrimaryShardCount) / (float) (state.getRoutingNodes().size());
        float minAvgNumberOfShardsSR = Math.round(Math.round(Math.floor(avgNumShardsSR - 1.0f)));
        float maxAvgNumberOfShardsSR = Math.round(Math.round(Math.ceil(avgNumShardsSR + 1.0f)));

        logger.info("--> Verify shard allocation with totalShards {} noOfNodes {} minAvg {} and maxAvg {} ", totalPrimaryShardCount, state.getRoutingNodes().size(), minAvgNumberOfShardsSR, maxAvgNumberOfShardsSR);
        shardAllocations.printShardDistribution(state);
//        for(ObjectObjectCursor<String, IndexMetadata> indexMetaData: state.metadata().indices().valuesIt()) {
//            if (indexMetaData.value.getSettings().get(IndexMetadata.SETTING_REPLICATION_TYPE).equals(ReplicationType.SEGMENT)) {
//                totalShardsCounterSR.updateAndGet(v -> new Float((float) (v + indexMetaData.value)))
//            }
//        }
        for (RoutingNode node : state.getRoutingNodes()) {
            List<ShardRouting> startedShards = node.primaryShardsWithState(STARTED);
            int segRepShardCount = 0;
            for(ShardRouting routing: startedShards) {
                if (segrepShardSet.contains(routing.shardId().getIndexName())) {
                    segRepShardCount++;
                }
            }
            assertTrue(segRepShardCount >= minAvgNumberOfShardsSR);
            assertTrue(segRepShardCount <= maxAvgNumberOfShardsSR);
        }
    }


    /**
     * This class is created for debugging purpose to show shard allocation across nodes. It keeps cluster state which
     * is used to build the node's shard allocation
     */
    private class ShardAllocations {
        ClusterState state;

        public static final String separator = "===================================================";
        public static final String ONE_LINE_RETURN = "\n";
        public static final String TWO_LINE_RETURN = "\n\n";

        /**
         Store shard primary/replica shard count against a node for segrep indices.
         String: NodeId
         int[]: tuple storing primary shard count in 0th index and replica's in 1
         */
        TreeMap<String, int[]> nodeToSegRepCountMap = new TreeMap<>();
        /**
         Store shard primary/replica shard count against a node for docrep indices.
         String: NodeId
         int[]: tuple storing primary shard count in 0th index and replica's in 1
         */
        TreeMap<String, int[]> nodeToDocRepCountMap = new TreeMap<>();

        /**
         * Helper map containing NodeName to NodeId
         */
        TreeMap<String, String> nameToNodeId = new TreeMap<>();

        /*
        Unassigned array containing primary at 0, replica at 1
         */
        int[] unassigned = new int[2];

        int[] totalShards = new int[2];

        public final String printShardAllocationWithHeader(int[] docrep, int[] segrep) {
            StringBuffer sb = new StringBuffer();
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            formatter.format("%-20s %-20s %-20s %-20s\n", "P", docrep[0] + segrep[0], docrep[0], segrep[0]);
            formatter.format("%-20s %-20s %-20s %-20s\n", "R", docrep[1] + segrep[1], docrep[1], segrep[1]);
            return sb.toString();
        }

        public void reset() {
            nodeToSegRepCountMap.clear();
            nodeToDocRepCountMap.clear();
            nameToNodeId.clear();
            totalShards[0] = totalShards[1] = 0;
            unassigned[0] = unassigned[1] = 0;
        }

        public void setState(ClusterState state) {
            this.reset();
            this.state = state;
            buildMap();
        }

        private void buildMap() {
            for (RoutingNode node : state.getRoutingNodes()) {
                nameToNodeId.putIfAbsent(node.node().getName(), node.nodeId());
                nodeToSegRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
                nodeToDocRepCountMap.putIfAbsent(node.nodeId(), new int[] { 0, 0 });
            }
            for (ShardRouting shardRouting : state.routingTable().allShards()) {
                // Fetch shard to update. Initialize local array
                if (isIndexSegRep(shardRouting.getIndexName())) {
                    updateMap(nodeToSegRepCountMap, shardRouting);
                } else {
                    updateMap(nodeToDocRepCountMap, shardRouting);
                }
            }
        }

        void updateMap(TreeMap<String, int[]> mapToUpdate, ShardRouting shardRouting) {
            int[] shard;
            shard = shardRouting.assignedToNode() ? mapToUpdate.get(shardRouting.currentNodeId()) : unassigned;
            // Update shard type count
            if (shardRouting.primary()) {
                shard[0]++;
                totalShards[0]++;
            } else {
                shard[1]++;
                totalShards[1]++;
            }
            // For assigned shards, put back counter
            if (shardRouting.assignedToNode()) mapToUpdate.put(shardRouting.currentNodeId(), shard);
        }

        boolean isIndexSegRep(String indexName) {
            return state.metadata()
                .index(indexName)
                .getSettings()
                .get(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey())
                .equals(ReplicationType.SEGMENT.toString());
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(TWO_LINE_RETURN + separator + ONE_LINE_RETURN);
            Formatter formatter = new Formatter(sb, Locale.getDefault());
            for (Map.Entry<String, String> entry : nameToNodeId.entrySet()) {
                String nodeId = nameToNodeId.get(entry.getKey());
                formatter.format("%-20s %-20s %-20s %-20s\n", entry.getKey().toUpperCase(Locale.getDefault()), "TOTAL", "DOCREP", "SEGREP");
                sb.append(printShardAllocationWithHeader(nodeToDocRepCountMap.get(nodeId), nodeToSegRepCountMap.get(nodeId)));
            }
            sb.append(ONE_LINE_RETURN);
            formatter.format("%-20s %-20s %-20s\n\n", "Unassigned ", unassigned[0], unassigned[1]);
            formatter.format("%-20s %-20s %-20s\n\n", "Total Shards", totalShards[0], totalShards[1]);
            return sb.toString();
        }

        public void printShardDistribution(ClusterState state) {
            this.setState(state);
            logger.info("--> Shard distribution {}", this);
        }
    }

}
