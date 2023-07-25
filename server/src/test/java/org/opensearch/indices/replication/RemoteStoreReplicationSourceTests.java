/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Version;
import org.mockito.Mockito;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.RemoteStoreRefreshListenerTests;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteStoreReplicationSourceTests extends OpenSearchIndexLevelReplicationTestCase {
    private static final long REPLICATION_ID = 123L;
    private RemoteStoreReplicationSource replicationSource;
    private IndexShard indexShard;

    private IndexShard mockShard;

    private Store remoteStore;

    private final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "my-repo")
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .build();

    @Override
    public void setUp() throws Exception {
        super.setUp();

        indexShard = newStartedShard(true, settings, new InternalEngineFactory());
        indexDoc(indexShard, "_doc", "1");
        indexDoc(indexShard, "_doc", "2");
        indexShard.refresh("test");

        // mock shard
        mockShard = mock(IndexShard.class);
        Store store = mock(Store.class);
        when(mockShard.store()).thenReturn(store);
        when(store.directory()).thenReturn(indexShard.store().directory());
        remoteStore = mock(Store.class);
        when(mockShard.remoteStore()).thenReturn(remoteStore);
        when(mockShard.state()).thenReturn(indexShard.state());
        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
            (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory()).getDelegate())
                .getDelegate();
        FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(
            new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory)
        );
        when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);
        replicationSource = new RemoteStoreReplicationSource(mockShard);
    }

    @Override
    public void tearDown() throws Exception {
        closeShards(indexShard);
        super.tearDown();
    }

    public void testGetCheckpointMetadata() throws ExecutionException, InterruptedException {
        when(mockShard.getSegmentInfosSnapshot()).thenReturn(indexShard.getSegmentInfosSnapshot());
        final ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();

        final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
        replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
        CheckpointInfoResponse response = res.get();
        assert (response.getCheckpoint().equals(checkpoint));
        assert (!response.getMetadataMap().isEmpty());
    }

    public void testGetCheckpointMetadataFailure() {
        final ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();

        when(mockShard.getSegmentInfosSnapshot()).thenThrow(new RuntimeException("test"));

        assertThrows(RuntimeException.class, () -> {
            final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
            res.get();
        });
    }

    public void testGetCheckpointMetadataEmpty() throws ExecutionException, InterruptedException, IOException {
        SegmentInfos segmentInfos = indexShard.getSegmentInfosSnapshot().get();
        when(mockShard.getSegmentInfosSnapshot()).thenReturn(indexShard.getSegmentInfosSnapshot());
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            indexShard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            segmentInfos.getVersion(),
            indexShard.store().getSegmentMetadataMap(segmentInfos).values().stream().mapToLong(StoreFileMetadata::length).sum(),
            indexShard.getDefaultCodecName()
        );
        IndexShard emptyIndexShard = null;
        try {
            emptyIndexShard = newStartedShard(
                true,
                settings,
                new InternalEngineFactory()
            );
            RemoteSegmentStoreDirectory remoteSegmentStoreDirectory =
                (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) emptyIndexShard.remoteStore().directory()).getDelegate())
                    .getDelegate();
            FilterDirectory remoteStoreFilterDirectory = new RemoteStoreRefreshListenerTests.TestFilterDirectory(
                new RemoteStoreRefreshListenerTests.TestFilterDirectory(remoteSegmentStoreDirectory)
            );
            when(remoteStore.directory()).thenReturn(remoteStoreFilterDirectory);

            final PlainActionFuture<CheckpointInfoResponse> res = PlainActionFuture.newFuture();
            when(mockShard.state()).thenReturn(IndexShardState.RECOVERING);
            replicationSource = new RemoteStoreReplicationSource(mockShard);
            // Recovering shard should just do a noop and return empty metadata map.
            replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res);
            CheckpointInfoResponse response = res.get();
            assert (response.getCheckpoint().equals(checkpoint));
            assert (response.getMetadataMap().isEmpty());

            when(mockShard.state()).thenReturn(IndexShardState.STARTED);
            // Started shard should fail with assertion error.
            expectThrows(AssertionError.class, () -> {
                final PlainActionFuture<CheckpointInfoResponse> res2 = PlainActionFuture.newFuture();
                replicationSource.getCheckpointMetadata(REPLICATION_ID, checkpoint, res2);
            });
        } finally {
            closeShards(emptyIndexShard);
        }
    }

    public void testGetSegmentFiles() throws ExecutionException, InterruptedException, IOException {
        SegmentInfos segmentInfos = indexShard.getSegmentInfosSnapshot().get();
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            indexShard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            segmentInfos.getVersion(),
            indexShard.store().getSegmentMetadataMap(segmentInfos).values().stream().mapToLong(StoreFileMetadata::length).sum(),
            indexShard.getDefaultCodecName()
        );
        List<StoreFileMetadata> filesToFetch = indexShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());

        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, indexShard, res);
        GetSegmentFilesResponse response = res.get();
        assert (response.files.isEmpty());
        assertEquals("RemoteStoreReplicationSource", replicationSource.getDescription());

    }

    public void testGetSegmentFilesFileAlreadyExists() throws IOException {
        SegmentInfos segmentInfos = indexShard.getSegmentInfosSnapshot().get();
        final ReplicationCheckpoint checkpoint = new ReplicationCheckpoint(
            indexShard.shardId(),
            indexShard.getOperationPrimaryTerm(),
            segmentInfos.getGeneration(),
            segmentInfos.getVersion(),
            indexShard.store().getSegmentMetadataMap(segmentInfos).values().stream().mapToLong(StoreFileMetadata::length).sum(),
            indexShard.getDefaultCodecName()
        );
        List<StoreFileMetadata> filesToFetch = indexShard.getSegmentMetadataMap().values().stream().collect(Collectors.toList());
//        assertThrows(FileAlreadyExistsException.class, () -> {
//            final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
//            replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, filesToFetch, indexShard, res);
//            res.get();
//        });
    }

    public void testGetSegmentFilesFailure() throws IOException {
        final ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();
        StoreFileMetadata testMetadata = new StoreFileMetadata("testFile", 1L, "checksum", Version.LATEST);
        Mockito.doThrow(new RuntimeException("testing")).when(mockShard).store();
        assertThrows(ExecutionException.class, () -> {
            final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
            replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, List.of(testMetadata), mockShard, res);
            res.get(10, TimeUnit.SECONDS);
        });
    }

    public void testGetSegmentFilesReturnEmptyResponse() throws ExecutionException, InterruptedException {
        final ReplicationCheckpoint checkpoint = indexShard.getLatestReplicationCheckpoint();
        final PlainActionFuture<GetSegmentFilesResponse> res = PlainActionFuture.newFuture();
        replicationSource.getSegmentFiles(REPLICATION_ID, checkpoint, Collections.emptyList(), mockShard, res);
        GetSegmentFilesResponse response = res.get();
        assert (response.files.isEmpty());
    }
}
