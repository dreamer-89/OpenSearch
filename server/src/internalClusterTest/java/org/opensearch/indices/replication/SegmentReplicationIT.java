/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Assert;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.Segment;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-idx-1";
    private static final int SHARD_COUNT = 1;
    private static final int REPLICA_COUNT = 1;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, REPLICA_COUNT)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        final String nodeA = internalCluster().startNode();
        final String nodeB = internalCluster().startNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 200);
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
            refresh(INDEX_NAME);

            // wait a short amount of time to give replication a chance to complete.
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), initialDocCount);

            final int additionalDocCount = scaledRandomIntBetween(0, 200);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            Thread.sleep(1000);
            assertHitCount(client(nodeA).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);
            assertHitCount(client(nodeB).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

            ensureGreen(INDEX_NAME);
            assertSegmentStats(REPLICA_COUNT);
        }
    }

    private void assertSegmentStats(int numberOfReplicas) {
        client().admin().indices().segments(new IndicesSegmentsRequest(), new ActionListener<>() {
            @Override
            public void onResponse(IndicesSegmentResponse indicesSegmentResponse) {

                List<ShardSegments[]> segmentsByIndex = indicesSegmentResponse.getIndices()
                    .values()
                    .stream() // get list of IndexSegments
                    .flatMap(is -> is.getShards().values().stream()) // Map to shard replication group
                    .map(IndexShardSegments::getShards) // get list of segments across replication group
                    .collect(Collectors.toList());

                // There will be an entry in the list for each index.
                for (ShardSegments[] replicationGroupSegments : segmentsByIndex) {

                    // Separate Primary & replica shards ShardSegments.
                    final Map<Boolean, List<ShardSegments>> segmentListMap = Arrays.stream(replicationGroupSegments)
                        .collect(Collectors.groupingBy(s -> s.getShardRouting().primary()));
                    final List<ShardSegments> primaryShardSegmentsList = segmentListMap.get(true);
                    final List<ShardSegments> replicaShardSegments = segmentListMap.get(false);

                    assertEquals("There should only be one primary in the replicationGroup", primaryShardSegmentsList.size(), 1);
                    final ShardSegments primaryShardSegments = primaryShardSegmentsList.stream().findFirst().get();

                    // create a map of the primary's segments keyed by segment name, allowing us to compare the same segment found on
                    // replicas.
                    final Map<String, Segment> primarySegmentsMap = primaryShardSegments.getSegments()
                        .stream()
                        .collect(Collectors.toMap(Segment::getName, Function.identity()));
                    // For every replica, ensure that its segments are in the same state as on the primary.
                    // It is possible the primary has not cleaned up old segments that are not required on replicas, so we can't do a
                    // list comparison.
                    // This equality check includes search/committed properties on the Segment. Combined with docCount checks,
                    // this ensures the replica has correctly copied the latest segments and has all segments referenced by the latest
                    // commit point, even if they are not searchable.
                    assertEquals(
                        "There should be a ShardSegment entry for each replica in the replicationGroup",
                        numberOfReplicas,
                        replicaShardSegments.size()
                    );

                    for (ShardSegments shardSegment : replicaShardSegments) {
                        for (Segment replicaSegment : shardSegment.getSegments()) {
                            final Segment primarySegment = primarySegmentsMap.get(replicaSegment.getName());
                            assertEquals("Replica's segment should be identical to primary's version", replicaSegment, primarySegment);
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                Assert.fail("Error fetching segment stats");
            }
        });
    }
}
