/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;

public class RemoteStoreSource implements SegmentReplicationSource {
    @Override
    public void getCheckpointMetadata(long replicationId, ReplicationCheckpoint checkpoint, ActionListener<CheckpointInfoResponse> listener) {
        // Make call to remote store to build new replication checkpoint metadata. This

        /*
        1. Request checkpoint. Same as input checkpoint
        2. Request remote store to get metadata map Map<String, StoreFileMetadata>.
        3. Request remote store to get SegmentInfos bytes.
        channel.sendResponse(
                new CheckpointInfoResponse(copyState.getCheckpoint(), copyState.getMetadataMap(), copyState.getInfosBytes())
            );
         */
    }

    @Override
    public void getSegmentFiles(long replicationId, ReplicationCheckpoint checkpoint, List<StoreFileMetadata> filesToFetch, Store store, ActionListener<GetSegmentFilesResponse> listener) {
        // Make call to remote store to fetch the metadata of all files to evaluate diff. Request files missing on local disk store.
    }

    @Override
    public void cancel() {
        SegmentReplicationSource.super.cancel();
    }
}
