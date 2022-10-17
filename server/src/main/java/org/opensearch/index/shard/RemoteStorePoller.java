/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.SegmentReplicationTargetService;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RemoteStorePoller {

    private final IndexShard indexShard;
    private final Store store;
    private final RemoteSegmentStoreDirectory remoteDirectory;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private Runnable storePoller;

    private final SegmentReplicationTargetService segmentReplicationTargetService;

    protected final Logger logger = LogManager.getLogger(getClass());
    public RemoteStorePoller(IndexShard indexShard, SegmentReplicationTargetService segmentReplicationTargetService) {
        this.indexShard = indexShard;
        this.store = indexShard.store();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) ((FilterDirectory) ((FilterDirectory) indexShard.remoteStore().directory())
            .getDelegate()).getDelegate();
        this.segmentReplicationTargetService = segmentReplicationTargetService;

        // Start poller for replicas
        if(indexShard.shardRouting.primary() == false) {
            // Start the poller process here
            this.storePoller = () -> {
                // Poll remote store poller and call onNewCheckpoint with latest meta data files. It is then onNewCheckpoint method responsibility
                // to either start or ignore the checkpoint
                logger.info("--> Dummy poller supposed to poll remote store");

                // Step 1. Poll remote store to fetch the latest replication checkpoint
                try {
                    String[] uploadedFiles = this.remoteDirectory.listAll();
                    logger.info("Fetched files from remote store {}", Arrays.toString(uploadedFiles));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Step 2. Pass on replication checkpoint to target service which decides if replication need to be started

            };
            this.executorService.scheduleAtFixedRate(this.storePoller, 0, 10, TimeUnit.SECONDS);
        }
    }

    public void shutdownPoller() {
        this.executorService.shutdown();
    }

    public void startPoller() {
        // Step 1. Poll remote store to fetch the latest replication checkpoint

        // Step 2. Pass on replication checkpoint to target service which decides if replication need to be started

    }
}
