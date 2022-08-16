/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.common.bytes.BytesArray;

import java.io.IOException;
import java.nio.file.Files;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MultiFileWriterTests extends OpenSearchTestCase {

    private Store store;
    private Directory directory, spyDir;
    private ReplicationLuceneIndex indexState;
    private MultiFileWriter multiFileWriter;
    private BytesReference bytesReference;
    private ShardId shardId;

    final IndexSettings SEGREP_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build()
    );

    final IndexSettings DOCREP_INDEX_SETTINGS = IndexSettingsModule.newIndexSettings(
        "index",
        Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.DOCUMENT)
            .build()
    );

    final StoreFileMetadata SEGMENTS_FILE = new StoreFileMetadata(IndexFileNames.SEGMENTS, 1L, "0", Version.LATEST);
    final StoreFileMetadata TEST_FILE = new StoreFileMetadata("testFile", 1L, "0", Version.LATEST);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", UUIDs.randomBase64UUID(), 0);
        directory = new NIOFSDirectory(Files.createTempDirectory(""));//mock(Directory.class);
        spyDir = spy(directory);
        store = new Store(shardId, SEGREP_INDEX_SETTINGS,spyDir, new DummyShardLock(shardId)); //mock(Store.class);
        Store spyStore = spy(store);
        //when(store.directory()).thenReturn(directory);
        //when(store.createVerifyingOutput(any(), any(), any())).thenReturn(mock(IndexOutput.class));
        indexState = new ReplicationLuceneIndex();
        multiFileWriter = new MultiFileWriter(spyStore, indexState, "", logger, () -> {});
        bytesReference = new BytesArray("test string");
    }

    public void testMultiFileWriterSegrepCallsFsyncSuccessful() throws IOException {
        //when(store.indexSettings()).thenReturn(SEGREP_INDEX_SETTINGS);
        //when(directory.listAll()).thenReturn(new String[] { SEGMENTS_FILE.name() });
        indexState.addFileDetail(SEGMENTS_FILE.name(), SEGMENTS_FILE.length(), false);
        multiFileWriter.writeFileChunk(SEGMENTS_FILE, 0, bytesReference, true);
        verify(spyDir, times(1)).sync(any());
    }

    public void testMultiFileWriterDocrepCallsFsyncSuccessful() throws IOException {
        when(store.indexSettings()).thenReturn(DOCREP_INDEX_SETTINGS);
        when(directory.listAll()).thenReturn(new String[] { TEST_FILE.name() });
        indexState.addFileDetail(TEST_FILE.name(), TEST_FILE.length(), false);
        multiFileWriter.writeFileChunk(TEST_FILE, 0, bytesReference, true);
        verify(directory, times(1)).sync(any());
    }

    public void testMultiFileWriterSegrepCallsFsyncSkipped() throws IOException {
        when(store.indexSettings()).thenReturn(SEGREP_INDEX_SETTINGS);
        when(directory.listAll()).thenReturn(new String[] { TEST_FILE.name() });
        indexState.addFileDetail(TEST_FILE.name(), TEST_FILE.length(), false);
        multiFileWriter.writeFileChunk(TEST_FILE, 0, bytesReference, true);
        verify(directory, times(0)).sync(any());
    }

}
