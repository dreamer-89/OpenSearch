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
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MultiFileWriterTests extends OpenSearchTestCase {

    private Store store;
    private Directory directory;
    private ReplicationLuceneIndex indexState;
    private MultiFileWriter multiFileWriter;
    private BytesReference bytesReference;
    private BytesRefIterator bytesRefIterator;

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
        store = mock(Store.class);
        directory = mock(Directory.class);
        when(store.directory()).thenReturn(directory);
        when(store.createVerifyingOutput(any(), any(), any())).thenReturn(mock(IndexOutput.class));
        indexState = new ReplicationLuceneIndex();
        multiFileWriter = new MultiFileWriter(store, indexState, "", logger, () -> {});
        bytesReference = mock(BytesReference.class);
        bytesRefIterator = mock(BytesRefIterator.class);
        when(bytesReference.iterator()).thenReturn(bytesRefIterator);
        when(bytesRefIterator.next()).thenReturn(null);
    }

    public void testMultiFileWriterSegrepCallsFsyncSuccessful() throws IOException {
        when(store.indexSettings()).thenReturn(SEGREP_INDEX_SETTINGS);
        when(directory.listAll()).thenReturn(new String[] { SEGMENTS_FILE.name() });
        indexState.addFileDetail(SEGMENTS_FILE.name(), SEGMENTS_FILE.length(), false);
        multiFileWriter.writeFileChunk(SEGMENTS_FILE, 0, bytesReference, true);
        verify(directory, times(1)).sync(any());
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
