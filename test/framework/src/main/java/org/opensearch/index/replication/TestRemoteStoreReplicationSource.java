/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.action.ActionListener;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.RemoteStoreReplicationSource;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;

/**
 * Defines test SegmentReplicationSource for remote store to be used in unit tests
 */
public abstract class TestRemoteStoreReplicationSource implements SegmentReplicationSource {

    private static final Logger logger = LogManager.getLogger(TestRemoteStoreReplicationSource.class);

    private CancellableThreads cancellableThreads;
    private IndexShard targetShard;

    public CancellableThreads getCancellableThreads() {
        return cancellableThreads;
    }

    public IndexShard getTargetShard() {
        return targetShard;
    }

    public RemoteSegmentStoreDirectory getRemoteDirectory() {
        return remoteDirectory;
    }

    private RemoteSegmentStoreDirectory remoteDirectory;

    public TestRemoteStoreReplicationSource(CancellableThreads cancellableThreads, IndexShard targetShard) {
        logger.info("--> TestReplicationSource {}", cancellableThreads);
        this.targetShard = targetShard;
        FilterDirectory remoteStoreDirectory = (FilterDirectory) targetShard.remoteStore().directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();
        this.cancellableThreads = cancellableThreads;
    }

    public TestRemoteStoreReplicationSource() {
        logger.info("--> TestReplicationSource default");
    }

    @Override
    public abstract void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    );

    @Override
    public abstract void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        ActionListener<GetSegmentFilesResponse> listener
    );

    @Override
    public String getDescription() {
        return "TestReplicationSource";
    }
}
