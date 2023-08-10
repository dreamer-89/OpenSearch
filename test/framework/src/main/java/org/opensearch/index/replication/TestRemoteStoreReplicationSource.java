/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.replication;

import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.CheckpointInfoResponse;
import org.opensearch.indices.replication.GetSegmentFilesResponse;
import org.opensearch.indices.replication.SegmentReplicationSource;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;

/**
 * Defines SegmentReplicationSource for remote store to be used in unit tests
 */
public abstract class TestRemoteStoreReplicationSource implements SegmentReplicationSource {

    private final CancellableThreads cancellableThreads;
    private final IndexShard targetShard;

    private final RemoteSegmentStoreDirectory remoteDirectory;

    public RemoteSegmentStoreDirectory getRemoteDirectory() {
        return remoteDirectory;
    }

    public TestRemoteStoreReplicationSource(CancellableThreads cancellableThreads, IndexShard targetShard) {
        this.targetShard = targetShard;
        FilterDirectory remoteStoreDirectory = (FilterDirectory) targetShard.remoteStore().directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();
        this.cancellableThreads = cancellableThreads;
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
        return "TestRemoteStoreReplicationSource";
    }
}
