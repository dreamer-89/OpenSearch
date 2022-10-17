/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.BeforeClass;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.Nullable;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.replication.SegmentReplicationSourceService;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationWithRemoteStoreIT extends AbstractSnapshotIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 2;

    @SuppressForbidden(reason = "sets the feature flag")
    @BeforeClass
    public static void enableFeature() {
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(FeatureFlags.REPLICATION_TYPE, "true"));
        AccessController.doPrivileged((PrivilegedAction<String>) () -> System.setProperty(FeatureFlags.REMOTE_STORE, "true"));
    }

    @BeforeClass
    public static void assumeFeatureFlag() {
        assumeTrue("Segment replication Feature flag is enabled", Boolean.parseBoolean(System.getProperty(FeatureFlags.REPLICATION_TYPE)));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, "remote_store_repo")
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void indexDocuments(int initialDocCount) throws Exception {
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
        }
    }

    public void printShardStore(String nodeName) throws IOException {
        IndexShard shard = getIndexShard(nodeName);
        logger.info("--> {} files content {}", nodeName, Arrays.toString(shard.store().directory().listAll()));
    }

    public void testBasicIntegRemoteStore() throws Exception {
        // Snapshot declaration
        Path absolutePath = randomRepoPath().toAbsolutePath();
        // Create snapshot
        createRepository("remote_store_repo", "fs", absolutePath);

        final String primary = internalCluster().startNode();
        createIndex(INDEX_NAME);
        final String replica = internalCluster().startNode();
        ensureGreen(INDEX_NAME);

        final ClusterState clusterManagerState = internalCluster().clusterService().state();
        logger.info("--> Cluster state {}", clusterManagerState);

        // Index documents and perform flush
        indexDocuments(scaledRandomIntBetween(0, 200));
        logger.info("--> Before flush");
        printShardStore(primary);
        printShardStore(replica);
        logger.info("--> After flush");
        flush(INDEX_NAME);
        printShardStore(primary);
        printShardStore(replica);

        // Again index more documents and perform flush to generate a new commit point
        indexDocuments(scaledRandomIntBetween(0, 200));
        logger.info("--> Before flush");
        printShardStore(primary);
        printShardStore(replica);
        logger.info("--> After flush");
        flush(INDEX_NAME);
        printShardStore(primary);
        printShardStore(replica);


        Thread.sleep(10000);

            // wait a short amount of time to give replication a chance to complete.
//            assertHitCount(client(primary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
//            assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
    }

    private void assertSegmentStats(int numberOfReplicas) throws IOException {
        final IndicesSegmentResponse indicesSegmentResponse = client().admin().indices().segments(new IndicesSegmentsRequest()).actionGet();

        List<ShardSegments[]> segmentsByIndex = getShardSegments(indicesSegmentResponse);

        // There will be an entry in the list for each index.
        for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {

            // Separate Primary & replica shards ShardSegments.
            final Map<Boolean, List<ShardSegments>> segmentListMap = segmentsByShardType(replicationGroupSegments);
            final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
            final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);

            assertEquals("There should only be one primary in the replicationGroup", primaryShardSegmentsList.size(), 1);
            final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();
            final Map<String, Segment> latestPrimarySegments = getLatestSegments(primaryShardSegments);

            assertEquals(
                "There should be a ShardSegment entry for each replica in the replicationGroup",
                numberOfReplicas,
                replicaShardSegments.size()
            );

            for (ShardSegments shardSegment : replicaShardSegments) {
                final Map<String, Segment> latestReplicaSegments = getLatestSegments(shardSegment);
                for (Segment replicaSegment : latestReplicaSegments.values()) {
                    final Segment primarySegment = latestPrimarySegments.get(replicaSegment.getName());
                    assertEquals(replicaSegment.getGeneration(), primarySegment.getGeneration());
                    assertEquals(replicaSegment.getNumDocs(), primarySegment.getNumDocs());
                    assertEquals(replicaSegment.getDeletedDocs(), primarySegment.getDeletedDocs());
                    assertEquals(replicaSegment.getSize(), primarySegment.getSize());
                }

                // Fetch the IndexShard for this replica and try and build its SegmentInfos from the previous commit point.
                // This ensures the previous commit point is not wiped.
                final ShardRouting replicaShardRouting = shardSegment.getShardRouting();
                ClusterState state = client(internalCluster().getMasterName()).admin().cluster().prepareState().get().getState();
                final DiscoveryNode replicaNode = state.nodes().resolveNode(replicaShardRouting.currentNodeId());
                IndexShard indexShard = getIndexShard(replicaNode.getName());
                // calls to readCommit will fail if a valid commit point and all its segments are not in the store.
                indexShard.store().readLastCommittedSegmentsInfo();
            }
        }
    }

    /**
     * Waits until the replica is caught up to the latest primary segments gen.
     * @throws Exception if assertion fails
     */
    private void waitForReplicaUpdate() throws Exception {
        // wait until the replica has the latest segment generation.
        assertBusy(() -> {
            final IndicesSegmentResponse indicesSegmentResponse = client().admin()
                .indices()
                .segments(new IndicesSegmentsRequest())
                .actionGet();
            List<ShardSegments[]> segmentsByIndex = getShardSegments(indicesSegmentResponse);
            for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {
                final Map<Boolean, List<ShardSegments>> segmentListMap = segmentsByShardType(replicationGroupSegments);
                final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
                final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);
                // if we don't have any segments yet, proceed.
                final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();
                logger.debug("Primary Segments: {}", primaryShardSegments.getSegments());
                if (primaryShardSegments.getSegments().isEmpty() == false) {
                    final Map<String, Segment> latestPrimarySegments = getLatestSegments(primaryShardSegments);
                    final Long latestPrimaryGen = latestPrimarySegments.values().stream().findFirst().map(Segment::getGeneration).get();
                    for (ShardSegments shardSegments : replicaShardSegments) {
                        logger.debug("Replica {} Segments: {}", shardSegments.getShardRouting(), shardSegments.getSegments());
                        final boolean isReplicaCaughtUpToPrimary = shardSegments.getSegments()
                            .stream()
                            .anyMatch(segment -> segment.getGeneration() == latestPrimaryGen);
                        assertTrue(isReplicaCaughtUpToPrimary);
                    }
                }
            }
        });
    }

    private IndexShard getIndexShard(String node) {
        final Index index = resolveIndex(INDEX_NAME);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(index);
        final Optional<Integer> shardId = indexService.shardIds().stream().findFirst();
        return indexService.getShard(shardId.get());
    }

    private List<ShardSegments[]> getShardSegments(IndicesSegmentResponse indicesSegmentResponse) {
        return indicesSegmentResponse.getIndices()
            .values()
            .stream() // get list of IndexSegments
            .flatMap(is -> is.getShards().values().stream()) // Map to shard replication group
            .map(IndexShardSegments::getShards) // get list of segments across replication group
            .collect(Collectors.toList());
    }

    private Map<String, Segment> getLatestSegments(ShardSegments segments) {
        final Optional<Long> generation = segments.getSegments().stream().map(Segment::getGeneration).max(Long::compare);
        final Long latestPrimaryGen = generation.get();
        return segments.getSegments()
            .stream()
            .filter(s -> s.getGeneration() == latestPrimaryGen)
            .collect(Collectors.toMap(Segment::getName, Function.identity()));
    }

    private Map<Boolean, List<ShardSegments>> segmentsByShardType(ShardSegments[] replicationGroupSegments) {
        return Arrays.stream(replicationGroupSegments).collect(Collectors.groupingBy(s -> s.getShardRouting().primary()));
    }

    @Nullable
    private ShardRouting getShardRoutingForNodeName(String nodeName) {
        final ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        for (IndexShardRoutingTable shardRoutingTable : state.routingTable().index(INDEX_NAME)) {
            for (ShardRouting shardRouting : shardRoutingTable.activeShards()) {
                final String nodeId = shardRouting.currentNodeId();
                final DiscoveryNode discoveryNode = state.nodes().resolveNode(nodeId);
                if (discoveryNode.getName().equals(nodeName)) {
                    return shardRouting;
                }
            }
        }
        return null;
    }

    private void assertDocCounts(int expectedDocCount, String... nodeNames) {
        for (String node : nodeNames) {
            assertHitCount(client(node).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedDocCount);
        }
    }

    private DiscoveryNode getNodeContainingPrimaryShard() {
        final ClusterState state = client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
        final ShardRouting primaryShard = state.routingTable().index(INDEX_NAME).shard(0).primaryShard();
        return state.nodes().resolveNode(primaryShard.currentNodeId());
    }
}
