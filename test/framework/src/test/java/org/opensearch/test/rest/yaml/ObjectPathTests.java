/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test.rest.yaml;

import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class ObjectPathTests extends OpenSearchTestCase {

    private static XContentBuilder randomXContentBuilder() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        return XContentBuilder.builder(XContentFactory.xContent(xContentType));
    }
    String getDummyResponse() {
        String response = "{\n" +
            "  \"_shards\" : {\n" +
            "    \"total\" : 2,\n" +
            "    \"successful\" : 2,\n" +
            "    \"failed\" : 0\n" +
            "  },\n" +
            "  \"_all\" : {\n" +
            "    \"primaries\" : {\n" +
            "      \"docs\" : {\n" +
            "        \"count\" : 5,\n" +
            "        \"deleted\" : 0\n" +
            "      },\n" +
            "      \"store\" : {\n" +
            "        \"size_in_bytes\" : 4553,\n" +
            "        \"reserved_in_bytes\" : 0\n" +
            "      },\n" +
            "      \"indexing\" : {\n" +
            "        \"index_total\" : 5,\n" +
            "        \"index_time_in_millis\" : 10,\n" +
            "        \"index_current\" : 0,\n" +
            "        \"index_failed\" : 0,\n" +
            "        \"delete_total\" : 0,\n" +
            "        \"delete_time_in_millis\" : 0,\n" +
            "        \"delete_current\" : 0,\n" +
            "        \"noop_update_total\" : 0,\n" +
            "        \"is_throttled\" : false,\n" +
            "        \"throttle_time_in_millis\" : 0\n" +
            "      },\n" +
            "      \"get\" : {\n" +
            "        \"total\" : 0,\n" +
            "        \"time_in_millis\" : 0,\n" +
            "        \"exists_total\" : 0,\n" +
            "        \"exists_time_in_millis\" : 0,\n" +
            "        \"missing_total\" : 0,\n" +
            "        \"missing_time_in_millis\" : 0,\n" +
            "        \"current\" : 0\n" +
            "      },\n" +
            "      \"search\" : {\n" +
            "        \"open_contexts\" : 0,\n" +
            "        \"query_total\" : 0,\n" +
            "        \"query_time_in_millis\" : 0,\n" +
            "        \"query_current\" : 0,\n" +
            "        \"fetch_total\" : 0,\n" +
            "        \"fetch_time_in_millis\" : 0,\n" +
            "        \"fetch_current\" : 0,\n" +
            "        \"scroll_total\" : 0,\n" +
            "        \"scroll_time_in_millis\" : 0,\n" +
            "        \"scroll_current\" : 0,\n" +
            "        \"point_in_time_total\" : 0,\n" +
            "        \"point_in_time_time_in_millis\" : 0,\n" +
            "        \"point_in_time_current\" : 0,\n" +
            "        \"suggest_total\" : 0,\n" +
            "        \"suggest_time_in_millis\" : 0,\n" +
            "        \"suggest_current\" : 0\n" +
            "      },\n" +
            "      \"merges\" : {\n" +
            "        \"current\" : 0,\n" +
            "        \"current_docs\" : 0,\n" +
            "        \"current_size_in_bytes\" : 0,\n" +
            "        \"total\" : 0,\n" +
            "        \"total_time_in_millis\" : 0,\n" +
            "        \"total_docs\" : 0,\n" +
            "        \"total_size_in_bytes\" : 0,\n" +
            "        \"total_stopped_time_in_millis\" : 0,\n" +
            "        \"total_throttled_time_in_millis\" : 0,\n" +
            "        \"total_auto_throttle_in_bytes\" : 20971520\n" +
            "      },\n" +
            "      \"refresh\" : {\n" +
            "        \"total\" : 7,\n" +
            "        \"total_time_in_millis\" : 29,\n" +
            "        \"external_total\" : 3,\n" +
            "        \"external_total_time_in_millis\" : 34,\n" +
            "        \"listeners\" : 0\n" +
            "      },\n" +
            "      \"flush\" : {\n" +
            "        \"total\" : 1,\n" +
            "        \"periodic\" : 0,\n" +
            "        \"total_time_in_millis\" : 7\n" +
            "      },\n" +
            "      \"warmer\" : {\n" +
            "        \"current\" : 0,\n" +
            "        \"total\" : 2,\n" +
            "        \"total_time_in_millis\" : 1\n" +
            "      },\n" +
            "      \"query_cache\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"total_count\" : 0,\n" +
            "        \"hit_count\" : 0,\n" +
            "        \"miss_count\" : 0,\n" +
            "        \"cache_size\" : 0,\n" +
            "        \"cache_count\" : 0,\n" +
            "        \"evictions\" : 0\n" +
            "      },\n" +
            "      \"fielddata\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"evictions\" : 0\n" +
            "      },\n" +
            "      \"completion\" : {\n" +
            "        \"size_in_bytes\" : 0\n" +
            "      },\n" +
            "      \"segments\" : {\n" +
            "        \"count\" : 1,\n" +
            "        \"memory_in_bytes\" : 0,\n" +
            "        \"terms_memory_in_bytes\" : 0,\n" +
            "        \"stored_fields_memory_in_bytes\" : 0,\n" +
            "        \"term_vectors_memory_in_bytes\" : 0,\n" +
            "        \"norms_memory_in_bytes\" : 0,\n" +
            "        \"points_memory_in_bytes\" : 0,\n" +
            "        \"doc_values_memory_in_bytes\" : 0,\n" +
            "        \"index_writer_memory_in_bytes\" : 0,\n" +
            "        \"version_map_memory_in_bytes\" : 0,\n" +
            "        \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "        \"max_unsafe_auto_id_timestamp\" : -1,\n" +
            "        \"file_sizes\" : { }\n" +
            "      },\n" +
            "      \"translog\" : {\n" +
            "        \"operations\" : 5,\n" +
            "        \"size_in_bytes\" : 505,\n" +
            "        \"uncommitted_operations\" : 5,\n" +
            "        \"uncommitted_size_in_bytes\" : 505,\n" +
            "        \"earliest_last_modified_age\" : 35\n" +
            "      },\n" +
            "      \"request_cache\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"evictions\" : 0,\n" +
            "        \"hit_count\" : 0,\n" +
            "        \"miss_count\" : 0\n" +
            "      },\n" +
            "      \"recovery\" : {\n" +
            "        \"current_as_source\" : 0,\n" +
            "        \"current_as_target\" : 0,\n" +
            "        \"throttle_time_in_millis\" : 0\n" +
            "      }\n" +
            "    },\n" +
            "    \"total\" : {\n" +
            "      \"docs\" : {\n" +
            "        \"count\" : 10,\n" +
            "        \"deleted\" : 0\n" +
            "      },\n" +
            "      \"store\" : {\n" +
            "        \"size_in_bytes\" : 4783,\n" +
            "        \"reserved_in_bytes\" : 0\n" +
            "      },\n" +
            "      \"indexing\" : {\n" +
            "        \"index_total\" : 5,\n" +
            "        \"index_time_in_millis\" : 10,\n" +
            "        \"index_current\" : 0,\n" +
            "        \"index_failed\" : 0,\n" +
            "        \"delete_total\" : 0,\n" +
            "        \"delete_time_in_millis\" : 0,\n" +
            "        \"delete_current\" : 0,\n" +
            "        \"noop_update_total\" : 0,\n" +
            "        \"is_throttled\" : false,\n" +
            "        \"throttle_time_in_millis\" : 0\n" +
            "      },\n" +
            "      \"get\" : {\n" +
            "        \"total\" : 0,\n" +
            "        \"time_in_millis\" : 0,\n" +
            "        \"exists_total\" : 0,\n" +
            "        \"exists_time_in_millis\" : 0,\n" +
            "        \"missing_total\" : 0,\n" +
            "        \"missing_time_in_millis\" : 0,\n" +
            "        \"current\" : 0\n" +
            "      },\n" +
            "      \"search\" : {\n" +
            "        \"open_contexts\" : 0,\n" +
            "        \"query_total\" : 0,\n" +
            "        \"query_time_in_millis\" : 0,\n" +
            "        \"query_current\" : 0,\n" +
            "        \"fetch_total\" : 0,\n" +
            "        \"fetch_time_in_millis\" : 0,\n" +
            "        \"fetch_current\" : 0,\n" +
            "        \"scroll_total\" : 0,\n" +
            "        \"scroll_time_in_millis\" : 0,\n" +
            "        \"scroll_current\" : 0,\n" +
            "        \"point_in_time_total\" : 0,\n" +
            "        \"point_in_time_time_in_millis\" : 0,\n" +
            "        \"point_in_time_current\" : 0,\n" +
            "        \"suggest_total\" : 0,\n" +
            "        \"suggest_time_in_millis\" : 0,\n" +
            "        \"suggest_current\" : 0\n" +
            "      },\n" +
            "      \"merges\" : {\n" +
            "        \"current\" : 0,\n" +
            "        \"current_docs\" : 0,\n" +
            "        \"current_size_in_bytes\" : 0,\n" +
            "        \"total\" : 0,\n" +
            "        \"total_time_in_millis\" : 0,\n" +
            "        \"total_docs\" : 0,\n" +
            "        \"total_size_in_bytes\" : 0,\n" +
            "        \"total_stopped_time_in_millis\" : 0,\n" +
            "        \"total_throttled_time_in_millis\" : 0,\n" +
            "        \"total_auto_throttle_in_bytes\" : 20971520\n" +
            "      },\n" +
            "      \"refresh\" : {\n" +
            "        \"total\" : 12,\n" +
            "        \"total_time_in_millis\" : 42,\n" +
            "        \"external_total\" : 8,\n" +
            "        \"external_total_time_in_millis\" : 47,\n" +
            "        \"listeners\" : 0\n" +
            "      },\n" +
            "      \"flush\" : {\n" +
            "        \"total\" : 2,\n" +
            "        \"periodic\" : 0,\n" +
            "        \"total_time_in_millis\" : 7\n" +
            "      },\n" +
            "      \"warmer\" : {\n" +
            "        \"current\" : 0,\n" +
            "        \"total\" : 2,\n" +
            "        \"total_time_in_millis\" : 1\n" +
            "      },\n" +
            "      \"query_cache\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"total_count\" : 0,\n" +
            "        \"hit_count\" : 0,\n" +
            "        \"miss_count\" : 0,\n" +
            "        \"cache_size\" : 0,\n" +
            "        \"cache_count\" : 0,\n" +
            "        \"evictions\" : 0\n" +
            "      },\n" +
            "      \"fielddata\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"evictions\" : 0\n" +
            "      },\n" +
            "      \"completion\" : {\n" +
            "        \"size_in_bytes\" : 0\n" +
            "      },\n" +
            "      \"segments\" : {\n" +
            "        \"count\" : 2,\n" +
            "        \"memory_in_bytes\" : 0,\n" +
            "        \"terms_memory_in_bytes\" : 0,\n" +
            "        \"stored_fields_memory_in_bytes\" : 0,\n" +
            "        \"term_vectors_memory_in_bytes\" : 0,\n" +
            "        \"norms_memory_in_bytes\" : 0,\n" +
            "        \"points_memory_in_bytes\" : 0,\n" +
            "        \"doc_values_memory_in_bytes\" : 0,\n" +
            "        \"index_writer_memory_in_bytes\" : 0,\n" +
            "        \"version_map_memory_in_bytes\" : 0,\n" +
            "        \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "        \"max_unsafe_auto_id_timestamp\" : -1,\n" +
            "        \"file_sizes\" : { }\n" +
            "      },\n" +
            "      \"translog\" : {\n" +
            "        \"operations\" : 10,\n" +
            "        \"size_in_bytes\" : 1010,\n" +
            "        \"uncommitted_operations\" : 10,\n" +
            "        \"uncommitted_size_in_bytes\" : 1010,\n" +
            "        \"earliest_last_modified_age\" : 35\n" +
            "      },\n" +
            "      \"request_cache\" : {\n" +
            "        \"memory_size_in_bytes\" : 0,\n" +
            "        \"evictions\" : 0,\n" +
            "        \"hit_count\" : 0,\n" +
            "        \"miss_count\" : 0\n" +
            "      },\n" +
            "      \"recovery\" : {\n" +
            "        \"current_as_source\" : 0,\n" +
            "        \"current_as_target\" : 0,\n" +
            "        \"throttle_time_in_millis\" : 0\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"indices\" : {\n" +
            "    \"test-index-segrep\" : {\n" +
            "      \"uuid\" : \"acxjepJZQMySA8BiOZPIlA\",\n" +
            "      \"primaries\" : {\n" +
            "        \"docs\" : {\n" +
            "          \"count\" : 5,\n" +
            "          \"deleted\" : 0\n" +
            "        },\n" +
            "        \"store\" : {\n" +
            "          \"size_in_bytes\" : 4553,\n" +
            "          \"reserved_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"indexing\" : {\n" +
            "          \"index_total\" : 5,\n" +
            "          \"index_time_in_millis\" : 10,\n" +
            "          \"index_current\" : 0,\n" +
            "          \"index_failed\" : 0,\n" +
            "          \"delete_total\" : 0,\n" +
            "          \"delete_time_in_millis\" : 0,\n" +
            "          \"delete_current\" : 0,\n" +
            "          \"noop_update_total\" : 0,\n" +
            "          \"is_throttled\" : false,\n" +
            "          \"throttle_time_in_millis\" : 0\n" +
            "        },\n" +
            "        \"get\" : {\n" +
            "          \"total\" : 0,\n" +
            "          \"time_in_millis\" : 0,\n" +
            "          \"exists_total\" : 0,\n" +
            "          \"exists_time_in_millis\" : 0,\n" +
            "          \"missing_total\" : 0,\n" +
            "          \"missing_time_in_millis\" : 0,\n" +
            "          \"current\" : 0\n" +
            "        },\n" +
            "        \"search\" : {\n" +
            "          \"open_contexts\" : 0,\n" +
            "          \"query_total\" : 0,\n" +
            "          \"query_time_in_millis\" : 0,\n" +
            "          \"query_current\" : 0,\n" +
            "          \"fetch_total\" : 0,\n" +
            "          \"fetch_time_in_millis\" : 0,\n" +
            "          \"fetch_current\" : 0,\n" +
            "          \"scroll_total\" : 0,\n" +
            "          \"scroll_time_in_millis\" : 0,\n" +
            "          \"scroll_current\" : 0,\n" +
            "          \"point_in_time_total\" : 0,\n" +
            "          \"point_in_time_time_in_millis\" : 0,\n" +
            "          \"point_in_time_current\" : 0,\n" +
            "          \"suggest_total\" : 0,\n" +
            "          \"suggest_time_in_millis\" : 0,\n" +
            "          \"suggest_current\" : 0\n" +
            "        },\n" +
            "        \"merges\" : {\n" +
            "          \"current\" : 0,\n" +
            "          \"current_docs\" : 0,\n" +
            "          \"current_size_in_bytes\" : 0,\n" +
            "          \"total\" : 0,\n" +
            "          \"total_time_in_millis\" : 0,\n" +
            "          \"total_docs\" : 0,\n" +
            "          \"total_size_in_bytes\" : 0,\n" +
            "          \"total_stopped_time_in_millis\" : 0,\n" +
            "          \"total_throttled_time_in_millis\" : 0,\n" +
            "          \"total_auto_throttle_in_bytes\" : 20971520\n" +
            "        },\n" +
            "        \"refresh\" : {\n" +
            "          \"total\" : 7,\n" +
            "          \"total_time_in_millis\" : 29,\n" +
            "          \"external_total\" : 3,\n" +
            "          \"external_total_time_in_millis\" : 34,\n" +
            "          \"listeners\" : 0\n" +
            "        },\n" +
            "        \"flush\" : {\n" +
            "          \"total\" : 1,\n" +
            "          \"periodic\" : 0,\n" +
            "          \"total_time_in_millis\" : 7\n" +
            "        },\n" +
            "        \"warmer\" : {\n" +
            "          \"current\" : 0,\n" +
            "          \"total\" : 2,\n" +
            "          \"total_time_in_millis\" : 1\n" +
            "        },\n" +
            "        \"query_cache\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"total_count\" : 0,\n" +
            "          \"hit_count\" : 0,\n" +
            "          \"miss_count\" : 0,\n" +
            "          \"cache_size\" : 0,\n" +
            "          \"cache_count\" : 0,\n" +
            "          \"evictions\" : 0\n" +
            "        },\n" +
            "        \"fielddata\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"evictions\" : 0\n" +
            "        },\n" +
            "        \"completion\" : {\n" +
            "          \"size_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"segments\" : {\n" +
            "          \"count\" : 1,\n" +
            "          \"memory_in_bytes\" : 0,\n" +
            "          \"terms_memory_in_bytes\" : 0,\n" +
            "          \"stored_fields_memory_in_bytes\" : 0,\n" +
            "          \"term_vectors_memory_in_bytes\" : 0,\n" +
            "          \"norms_memory_in_bytes\" : 0,\n" +
            "          \"points_memory_in_bytes\" : 0,\n" +
            "          \"doc_values_memory_in_bytes\" : 0,\n" +
            "          \"index_writer_memory_in_bytes\" : 0,\n" +
            "          \"version_map_memory_in_bytes\" : 0,\n" +
            "          \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "          \"max_unsafe_auto_id_timestamp\" : -1,\n" +
            "          \"file_sizes\" : { }\n" +
            "        },\n" +
            "        \"translog\" : {\n" +
            "          \"operations\" : 5,\n" +
            "          \"size_in_bytes\" : 505,\n" +
            "          \"uncommitted_operations\" : 5,\n" +
            "          \"uncommitted_size_in_bytes\" : 505,\n" +
            "          \"earliest_last_modified_age\" : 35\n" +
            "        },\n" +
            "        \"request_cache\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"evictions\" : 0,\n" +
            "          \"hit_count\" : 0,\n" +
            "          \"miss_count\" : 0\n" +
            "        },\n" +
            "        \"recovery\" : {\n" +
            "          \"current_as_source\" : 0,\n" +
            "          \"current_as_target\" : 0,\n" +
            "          \"throttle_time_in_millis\" : 0\n" +
            "        }\n" +
            "      },\n" +
            "      \"total\" : {\n" +
            "        \"docs\" : {\n" +
            "          \"count\" : 10,\n" +
            "          \"deleted\" : 0\n" +
            "        },\n" +
            "        \"store\" : {\n" +
            "          \"size_in_bytes\" : 4783,\n" +
            "          \"reserved_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"indexing\" : {\n" +
            "          \"index_total\" : 5,\n" +
            "          \"index_time_in_millis\" : 10,\n" +
            "          \"index_current\" : 0,\n" +
            "          \"index_failed\" : 0,\n" +
            "          \"delete_total\" : 0,\n" +
            "          \"delete_time_in_millis\" : 0,\n" +
            "          \"delete_current\" : 0,\n" +
            "          \"noop_update_total\" : 0,\n" +
            "          \"is_throttled\" : false,\n" +
            "          \"throttle_time_in_millis\" : 0\n" +
            "        },\n" +
            "        \"get\" : {\n" +
            "          \"total\" : 0,\n" +
            "          \"time_in_millis\" : 0,\n" +
            "          \"exists_total\" : 0,\n" +
            "          \"exists_time_in_millis\" : 0,\n" +
            "          \"missing_total\" : 0,\n" +
            "          \"missing_time_in_millis\" : 0,\n" +
            "          \"current\" : 0\n" +
            "        },\n" +
            "        \"search\" : {\n" +
            "          \"open_contexts\" : 0,\n" +
            "          \"query_total\" : 0,\n" +
            "          \"query_time_in_millis\" : 0,\n" +
            "          \"query_current\" : 0,\n" +
            "          \"fetch_total\" : 0,\n" +
            "          \"fetch_time_in_millis\" : 0,\n" +
            "          \"fetch_current\" : 0,\n" +
            "          \"scroll_total\" : 0,\n" +
            "          \"scroll_time_in_millis\" : 0,\n" +
            "          \"scroll_current\" : 0,\n" +
            "          \"point_in_time_total\" : 0,\n" +
            "          \"point_in_time_time_in_millis\" : 0,\n" +
            "          \"point_in_time_current\" : 0,\n" +
            "          \"suggest_total\" : 0,\n" +
            "          \"suggest_time_in_millis\" : 0,\n" +
            "          \"suggest_current\" : 0\n" +
            "        },\n" +
            "        \"merges\" : {\n" +
            "          \"current\" : 0,\n" +
            "          \"current_docs\" : 0,\n" +
            "          \"current_size_in_bytes\" : 0,\n" +
            "          \"total\" : 0,\n" +
            "          \"total_time_in_millis\" : 0,\n" +
            "          \"total_docs\" : 0,\n" +
            "          \"total_size_in_bytes\" : 0,\n" +
            "          \"total_stopped_time_in_millis\" : 0,\n" +
            "          \"total_throttled_time_in_millis\" : 0,\n" +
            "          \"total_auto_throttle_in_bytes\" : 20971520\n" +
            "        },\n" +
            "        \"refresh\" : {\n" +
            "          \"total\" : 12,\n" +
            "          \"total_time_in_millis\" : 42,\n" +
            "          \"external_total\" : 8,\n" +
            "          \"external_total_time_in_millis\" : 47,\n" +
            "          \"listeners\" : 0\n" +
            "        },\n" +
            "        \"flush\" : {\n" +
            "          \"total\" : 2,\n" +
            "          \"periodic\" : 0,\n" +
            "          \"total_time_in_millis\" : 7\n" +
            "        },\n" +
            "        \"warmer\" : {\n" +
            "          \"current\" : 0,\n" +
            "          \"total\" : 2,\n" +
            "          \"total_time_in_millis\" : 1\n" +
            "        },\n" +
            "        \"query_cache\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"total_count\" : 0,\n" +
            "          \"hit_count\" : 0,\n" +
            "          \"miss_count\" : 0,\n" +
            "          \"cache_size\" : 0,\n" +
            "          \"cache_count\" : 0,\n" +
            "          \"evictions\" : 0\n" +
            "        },\n" +
            "        \"fielddata\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"evictions\" : 0\n" +
            "        },\n" +
            "        \"completion\" : {\n" +
            "          \"size_in_bytes\" : 0\n" +
            "        },\n" +
            "        \"segments\" : {\n" +
            "          \"count\" : 2,\n" +
            "          \"memory_in_bytes\" : 0,\n" +
            "          \"terms_memory_in_bytes\" : 0,\n" +
            "          \"stored_fields_memory_in_bytes\" : 0,\n" +
            "          \"term_vectors_memory_in_bytes\" : 0,\n" +
            "          \"norms_memory_in_bytes\" : 0,\n" +
            "          \"points_memory_in_bytes\" : 0,\n" +
            "          \"doc_values_memory_in_bytes\" : 0,\n" +
            "          \"index_writer_memory_in_bytes\" : 0,\n" +
            "          \"version_map_memory_in_bytes\" : 0,\n" +
            "          \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "          \"max_unsafe_auto_id_timestamp\" : -1,\n" +
            "          \"file_sizes\" : { }\n" +
            "        },\n" +
            "        \"translog\" : {\n" +
            "          \"operations\" : 10,\n" +
            "          \"size_in_bytes\" : 1010,\n" +
            "          \"uncommitted_operations\" : 10,\n" +
            "          \"uncommitted_size_in_bytes\" : 1010,\n" +
            "          \"earliest_last_modified_age\" : 35\n" +
            "        },\n" +
            "        \"request_cache\" : {\n" +
            "          \"memory_size_in_bytes\" : 0,\n" +
            "          \"evictions\" : 0,\n" +
            "          \"hit_count\" : 0,\n" +
            "          \"miss_count\" : 0\n" +
            "        },\n" +
            "        \"recovery\" : {\n" +
            "          \"current_as_source\" : 0,\n" +
            "          \"current_as_target\" : 0,\n" +
            "          \"throttle_time_in_millis\" : 0\n" +
            "        }\n" +
            "      },\n" +
            "      \"shards\" : {\n" +
            "        \"0\" : [\n" +
            "          {\n" +
            "            \"routing\" : {\n" +
            "              \"state\" : \"STARTED\",\n" +
            "              \"primary\" : true,\n" +
            "              \"node\" : \"M3sl655hRtOXVASglo3EMQ\",\n" +
            "              \"relocating_node\" : null\n" +
            "            },\n" +
            "            \"docs\" : {\n" +
            "              \"count\" : 5,\n" +
            "              \"deleted\" : 0\n" +
            "            },\n" +
            "            \"store\" : {\n" +
            "              \"size_in_bytes\" : 4553,\n" +
            "              \"reserved_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"indexing\" : {\n" +
            "              \"index_total\" : 5,\n" +
            "              \"index_time_in_millis\" : 10,\n" +
            "              \"index_current\" : 0,\n" +
            "              \"index_failed\" : 0,\n" +
            "              \"delete_total\" : 0,\n" +
            "              \"delete_time_in_millis\" : 0,\n" +
            "              \"delete_current\" : 0,\n" +
            "              \"noop_update_total\" : 0,\n" +
            "              \"is_throttled\" : false,\n" +
            "              \"throttle_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"get\" : {\n" +
            "              \"total\" : 0,\n" +
            "              \"time_in_millis\" : 0,\n" +
            "              \"exists_total\" : 0,\n" +
            "              \"exists_time_in_millis\" : 0,\n" +
            "              \"missing_total\" : 0,\n" +
            "              \"missing_time_in_millis\" : 0,\n" +
            "              \"current\" : 0\n" +
            "            },\n" +
            "            \"search\" : {\n" +
            "              \"open_contexts\" : 0,\n" +
            "              \"query_total\" : 0,\n" +
            "              \"query_time_in_millis\" : 0,\n" +
            "              \"query_current\" : 0,\n" +
            "              \"fetch_total\" : 0,\n" +
            "              \"fetch_time_in_millis\" : 0,\n" +
            "              \"fetch_current\" : 0,\n" +
            "              \"scroll_total\" : 0,\n" +
            "              \"scroll_time_in_millis\" : 0,\n" +
            "              \"scroll_current\" : 0,\n" +
            "              \"point_in_time_total\" : 0,\n" +
            "              \"point_in_time_time_in_millis\" : 0,\n" +
            "              \"point_in_time_current\" : 0,\n" +
            "              \"suggest_total\" : 0,\n" +
            "              \"suggest_time_in_millis\" : 0,\n" +
            "              \"suggest_current\" : 0\n" +
            "            },\n" +
            "            \"merges\" : {\n" +
            "              \"current\" : 0,\n" +
            "              \"current_docs\" : 0,\n" +
            "              \"current_size_in_bytes\" : 0,\n" +
            "              \"total\" : 0,\n" +
            "              \"total_time_in_millis\" : 0,\n" +
            "              \"total_docs\" : 0,\n" +
            "              \"total_size_in_bytes\" : 0,\n" +
            "              \"total_stopped_time_in_millis\" : 0,\n" +
            "              \"total_throttled_time_in_millis\" : 0,\n" +
            "              \"total_auto_throttle_in_bytes\" : 20971520\n" +
            "            },\n" +
            "            \"refresh\" : {\n" +
            "              \"total\" : 7,\n" +
            "              \"total_time_in_millis\" : 29,\n" +
            "              \"external_total\" : 3,\n" +
            "              \"external_total_time_in_millis\" : 34,\n" +
            "              \"listeners\" : 0\n" +
            "            },\n" +
            "            \"flush\" : {\n" +
            "              \"total\" : 1,\n" +
            "              \"periodic\" : 0,\n" +
            "              \"total_time_in_millis\" : 7\n" +
            "            },\n" +
            "            \"warmer\" : {\n" +
            "              \"current\" : 0,\n" +
            "              \"total\" : 2,\n" +
            "              \"total_time_in_millis\" : 1\n" +
            "            },\n" +
            "            \"query_cache\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"total_count\" : 0,\n" +
            "              \"hit_count\" : 0,\n" +
            "              \"miss_count\" : 0,\n" +
            "              \"cache_size\" : 0,\n" +
            "              \"cache_count\" : 0,\n" +
            "              \"evictions\" : 0\n" +
            "            },\n" +
            "            \"fielddata\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"evictions\" : 0\n" +
            "            },\n" +
            "            \"completion\" : {\n" +
            "              \"size_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"segments\" : {\n" +
            "              \"count\" : 1,\n" +
            "              \"memory_in_bytes\" : 0,\n" +
            "              \"terms_memory_in_bytes\" : 0,\n" +
            "              \"stored_fields_memory_in_bytes\" : 0,\n" +
            "              \"term_vectors_memory_in_bytes\" : 0,\n" +
            "              \"norms_memory_in_bytes\" : 0,\n" +
            "              \"points_memory_in_bytes\" : 0,\n" +
            "              \"doc_values_memory_in_bytes\" : 0,\n" +
            "              \"index_writer_memory_in_bytes\" : 0,\n" +
            "              \"version_map_memory_in_bytes\" : 0,\n" +
            "              \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "              \"max_unsafe_auto_id_timestamp\" : -1,\n" +
            "              \"file_sizes\" : { }\n" +
            "            },\n" +
            "            \"translog\" : {\n" +
            "              \"operations\" : 5,\n" +
            "              \"size_in_bytes\" : 505,\n" +
            "              \"uncommitted_operations\" : 5,\n" +
            "              \"uncommitted_size_in_bytes\" : 505,\n" +
            "              \"earliest_last_modified_age\" : 35\n" +
            "            },\n" +
            "            \"request_cache\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"evictions\" : 0,\n" +
            "              \"hit_count\" : 0,\n" +
            "              \"miss_count\" : 0\n" +
            "            },\n" +
            "            \"recovery\" : {\n" +
            "              \"current_as_source\" : 0,\n" +
            "              \"current_as_target\" : 0,\n" +
            "              \"throttle_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"commit\" : {\n" +
            "              \"id\" : \"fG4arsS04B3sqaAX8UuDnw==\",\n" +
            "              \"generation\" : 3,\n" +
            "              \"user_data\" : {\n" +
            "                \"local_checkpoint\" : \"-1\",\n" +
            "                \"max_unsafe_auto_id_timestamp\" : \"-1\",\n" +
            "                \"min_retained_seq_no\" : \"0\",\n" +
            "                \"translog_uuid\" : \"P9iWLO19SFClvgTGCJ3SqA\",\n" +
            "                \"history_uuid\" : \"B4dY6_UtSR-oH9RHnY_Rwg\",\n" +
            "                \"max_seq_no\" : \"-1\"\n" +
            "              },\n" +
            "              \"num_docs\" : 0\n" +
            "            },\n" +
            "            \"seq_no\" : {\n" +
            "              \"max_seq_no\" : 4,\n" +
            "              \"local_checkpoint\" : 4,\n" +
            "              \"global_checkpoint\" : 4\n" +
            "            },\n" +
            "            \"retention_leases\" : {\n" +
            "              \"primary_term\" : 1,\n" +
            "              \"version\" : 2,\n" +
            "              \"leases\" : [\n" +
            "                {\n" +
            "                  \"id\" : \"peer_recovery/M3sl655hRtOXVASglo3EMQ\",\n" +
            "                  \"retaining_seq_no\" : 0,\n" +
            "                  \"timestamp\" : 1687316532449,\n" +
            "                  \"source\" : \"peer recovery\"\n" +
            "                },\n" +
            "                {\n" +
            "                  \"id\" : \"peer_recovery/_Fw5CbqPT7CmYwI6jlrnGg\",\n" +
            "                  \"retaining_seq_no\" : 0,\n" +
            "                  \"timestamp\" : 1687316532649,\n" +
            "                  \"source\" : \"peer recovery\"\n" +
            "                }\n" +
            "              ]\n" +
            "            },\n" +
            "            \"shard_path\" : {\n" +
            "              \"state_path\" : \"/home/ubuntu/OpenSearch/qa/rolling-upgrade/build/testclusters/v2.8.0-2/data/nodes/0\",\n" +
            "              \"data_path\" : \"/home/ubuntu/OpenSearch/qa/rolling-upgrade/build/testclusters/v2.8.0-2/data/nodes/0\",\n" +
            "              \"is_custom_data_path\" : false\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"routing\" : {\n" +
            "              \"state\" : \"STARTED\",\n" +
            "              \"primary\" : false,\n" +
            "              \"node\" : \"_Fw5CbqPT7CmYwI6jlrnGg\",\n" +
            "              \"relocating_node\" : null\n" +
            "            },\n" +
            "            \"docs\" : {\n" +
            "              \"count\" : 5,\n" +
            "              \"deleted\" : 0\n" +
            "            },\n" +
            "            \"store\" : {\n" +
            "              \"size_in_bytes\" : 230,\n" +
            "              \"reserved_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"indexing\" : {\n" +
            "              \"index_total\" : 0,\n" +
            "              \"index_time_in_millis\" : 0,\n" +
            "              \"index_current\" : 0,\n" +
            "              \"index_failed\" : 0,\n" +
            "              \"delete_total\" : 0,\n" +
            "              \"delete_time_in_millis\" : 0,\n" +
            "              \"delete_current\" : 0,\n" +
            "              \"noop_update_total\" : 0,\n" +
            "              \"is_throttled\" : false,\n" +
            "              \"throttle_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"get\" : {\n" +
            "              \"total\" : 0,\n" +
            "              \"time_in_millis\" : 0,\n" +
            "              \"exists_total\" : 0,\n" +
            "              \"exists_time_in_millis\" : 0,\n" +
            "              \"missing_total\" : 0,\n" +
            "              \"missing_time_in_millis\" : 0,\n" +
            "              \"current\" : 0\n" +
            "            },\n" +
            "            \"search\" : {\n" +
            "              \"open_contexts\" : 0,\n" +
            "              \"query_total\" : 0,\n" +
            "              \"query_time_in_millis\" : 0,\n" +
            "              \"query_current\" : 0,\n" +
            "              \"fetch_total\" : 0,\n" +
            "              \"fetch_time_in_millis\" : 0,\n" +
            "              \"fetch_current\" : 0,\n" +
            "              \"scroll_total\" : 0,\n" +
            "              \"scroll_time_in_millis\" : 0,\n" +
            "              \"scroll_current\" : 0,\n" +
            "              \"point_in_time_total\" : 0,\n" +
            "              \"point_in_time_time_in_millis\" : 0,\n" +
            "              \"point_in_time_current\" : 0,\n" +
            "              \"suggest_total\" : 0,\n" +
            "              \"suggest_time_in_millis\" : 0,\n" +
            "              \"suggest_current\" : 0\n" +
            "            },\n" +
            "            \"merges\" : {\n" +
            "              \"current\" : 0,\n" +
            "              \"current_docs\" : 0,\n" +
            "              \"current_size_in_bytes\" : 0,\n" +
            "              \"total\" : 0,\n" +
            "              \"total_time_in_millis\" : 0,\n" +
            "              \"total_docs\" : 0,\n" +
            "              \"total_size_in_bytes\" : 0,\n" +
            "              \"total_stopped_time_in_millis\" : 0,\n" +
            "              \"total_throttled_time_in_millis\" : 0,\n" +
            "              \"total_auto_throttle_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"refresh\" : {\n" +
            "              \"total\" : 5,\n" +
            "              \"total_time_in_millis\" : 13,\n" +
            "              \"external_total\" : 5,\n" +
            "              \"external_total_time_in_millis\" : 13,\n" +
            "              \"listeners\" : 0\n" +
            "            },\n" +
            "            \"flush\" : {\n" +
            "              \"total\" : 1,\n" +
            "              \"periodic\" : 0,\n" +
            "              \"total_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"warmer\" : {\n" +
            "              \"current\" : 0,\n" +
            "              \"total\" : 0,\n" +
            "              \"total_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"query_cache\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"total_count\" : 0,\n" +
            "              \"hit_count\" : 0,\n" +
            "              \"miss_count\" : 0,\n" +
            "              \"cache_size\" : 0,\n" +
            "              \"cache_count\" : 0,\n" +
            "              \"evictions\" : 0\n" +
            "            },\n" +
            "            \"fielddata\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"evictions\" : 0\n" +
            "            },\n" +
            "            \"completion\" : {\n" +
            "              \"size_in_bytes\" : 0\n" +
            "            },\n" +
            "            \"segments\" : {\n" +
            "              \"count\" : 1,\n" +
            "              \"memory_in_bytes\" : 0,\n" +
            "              \"terms_memory_in_bytes\" : 0,\n" +
            "              \"stored_fields_memory_in_bytes\" : 0,\n" +
            "              \"term_vectors_memory_in_bytes\" : 0,\n" +
            "              \"norms_memory_in_bytes\" : 0,\n" +
            "              \"points_memory_in_bytes\" : 0,\n" +
            "              \"doc_values_memory_in_bytes\" : 0,\n" +
            "              \"index_writer_memory_in_bytes\" : 0,\n" +
            "              \"version_map_memory_in_bytes\" : 0,\n" +
            "              \"fixed_bit_set_memory_in_bytes\" : 0,\n" +
            "              \"max_unsafe_auto_id_timestamp\" : -9223372036854775808,\n" +
            "              \"file_sizes\" : { }\n" +
            "            },\n" +
            "            \"translog\" : {\n" +
            "              \"operations\" : 5,\n" +
            "              \"size_in_bytes\" : 505,\n" +
            "              \"uncommitted_operations\" : 5,\n" +
            "              \"uncommitted_size_in_bytes\" : 505,\n" +
            "              \"earliest_last_modified_age\" : 60\n" +
            "            },\n" +
            "            \"request_cache\" : {\n" +
            "              \"memory_size_in_bytes\" : 0,\n" +
            "              \"evictions\" : 0,\n" +
            "              \"hit_count\" : 0,\n" +
            "              \"miss_count\" : 0\n" +
            "            },\n" +
            "            \"recovery\" : {\n" +
            "              \"current_as_source\" : 0,\n" +
            "              \"current_as_target\" : 0,\n" +
            "              \"throttle_time_in_millis\" : 0\n" +
            "            },\n" +
            "            \"commit\" : {\n" +
            "              \"id\" : \"GHViacddOHvymmPhzbPIkQ==\",\n" +
            "              \"generation\" : 4,\n" +
            "              \"user_data\" : {\n" +
            "                \"local_checkpoint\" : \"-1\",\n" +
            "                \"max_unsafe_auto_id_timestamp\" : \"-1\",\n" +
            "                \"min_retained_seq_no\" : \"0\",\n" +
            "                \"translog_uuid\" : \"P9iWLO19SFClvgTGCJ3SqA\",\n" +
            "                \"history_uuid\" : \"B4dY6_UtSR-oH9RHnY_Rwg\",\n" +
            "                \"max_seq_no\" : \"-1\"\n" +
            "              },\n" +
            "              \"num_docs\" : 0\n" +
            "            },\n" +
            "            \"seq_no\" : {\n" +
            "              \"max_seq_no\" : 4,\n" +
            "              \"local_checkpoint\" : 4,\n" +
            "              \"global_checkpoint\" : 4\n" +
            "            },\n" +
            "            \"retention_leases\" : {\n" +
            "              \"primary_term\" : 1,\n" +
            "              \"version\" : 2,\n" +
            "              \"leases\" : [\n" +
            "                {\n" +
            "                  \"id\" : \"peer_recovery/M3sl655hRtOXVASglo3EMQ\",\n" +
            "                  \"retaining_seq_no\" : 0,\n" +
            "                  \"timestamp\" : 1687316532449,\n" +
            "                  \"source\" : \"peer recovery\"\n" +
            "                },\n" +
            "                {\n" +
            "                  \"id\" : \"peer_recovery/_Fw5CbqPT7CmYwI6jlrnGg\",\n" +
            "                  \"retaining_seq_no\" : 0,\n" +
            "                  \"timestamp\" : 1687316532649,\n" +
            "                  \"source\" : \"peer recovery\"\n" +
            "                }\n" +
            "              ]\n" +
            "            },\n" +
            "            \"shard_path\" : {\n" +
            "              \"state_path\" : \"/home/ubuntu/OpenSearch/qa/rolling-upgrade/build/testclusters/v2.8.0-0/data/nodes/0\",\n" +
            "              \"data_path\" : \"/home/ubuntu/OpenSearch/qa/rolling-upgrade/build/testclusters/v2.8.0-0/data/nodes/0\",\n" +
            "              \"is_custom_data_path\" : false\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        return response;
    }

    public void testDummy() throws IOException {
        XContentType type = XContentType.JSON;
        ObjectPath objectPath = ObjectPath.createFromXContent(
            type.xContent(),
            new BytesArray(getDummyResponse().getBytes())
        );

        List<Object> objects = objectPath.evaluate("indices." + "test-index-segrep" + ".shards.0");
        for(Object shard: objects) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            System.out.println("nodeId " + nodeId + ", primary " + primary);
        }
    }
    public void testEvaluateObjectPathEscape() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2.field3", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.humanReadable(true);
        xContentBuilder.prettyPrint();
        System.out.println(Strings.toString(xContentBuilder));
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1.field2\\.field3");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    public void testEvaluateObjectPathWithDots() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1..field2");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("field1.field2.");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    public void testEvaluateInteger() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", 333);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Integer.class));
        assertThat(object, equalTo(333));
    }

    public void testEvaluateDouble() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.field("field2", 3.55);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1.field2");
        assertThat(object, instanceOf(Double.class));
        assertThat(object, equalTo(3.55));
    }

    public void testEvaluateArray() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.array("array1", "value1", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1.array1");
        assertThat(object, instanceOf(List.class));
        List list = (List) object;
        assertThat(list.size(), equalTo(2));
        assertThat(list.get(0), instanceOf(String.class));
        assertThat(list.get(0), equalTo("value1"));
        assertThat(list.get(1), instanceOf(String.class));
        assertThat(list.get(1), equalTo("value2"));
        object = objectPath.evaluate("field1.array1.1");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateArrayElementObject() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.startArray("array1");
        xContentBuilder.startObject();
        xContentBuilder.field("element", "value1");
        xContentBuilder.endObject();
        xContentBuilder.startObject();
        xContentBuilder.field("element", "value2");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("field1.array1.1.element");
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("value2"));
        object = objectPath.evaluate("");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Map.class));
        assertThat(((Map<String, Object>) object).containsKey("field1"), equalTo(true));
        object = objectPath.evaluate("field1.array2.1.element");
        assertThat(object, nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateObjectKeys() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("metadata");
        xContentBuilder.startObject("templates");
        xContentBuilder.startObject("template_1");
        xContentBuilder.field("field", "value");
        xContentBuilder.endObject();
        xContentBuilder.startObject("template_2");
        xContentBuilder.field("field", "value");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("metadata.templates");
        assertThat(object, instanceOf(Map.class));
        Map<String, Object> map = (Map<String, Object>) object;
        assertThat(map.size(), equalTo(2));
        Set<String> strings = map.keySet();
        assertThat(strings, contains("template_1", "template_2"));
    }

    public void testEvaluateArbitraryKey() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("metadata");
        xContentBuilder.startObject("templates");
        xContentBuilder.startObject("template_1");
        xContentBuilder.field("field1", "value");
        xContentBuilder.endObject();
        xContentBuilder.startObject("template_2");
        xContentBuilder.field("field2", "value");
        xContentBuilder.field("field3", "value");
        xContentBuilder.endObject();
        xContentBuilder.startObject("template_3");
        xContentBuilder.endObject();
        xContentBuilder.startObject("template_4");
        xContentBuilder.field("_arbitrary_key_", "value");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );

        {
            final Object object = objectPath.evaluate("metadata.templates.template_1._arbitrary_key_");
            assertThat(object, instanceOf(String.class));
            final String key = (String) object;
            assertThat(key, equalTo("field1"));
        }

        {
            final Object object = objectPath.evaluate("metadata.templates.template_2._arbitrary_key_");
            assertThat(object, instanceOf(String.class));
            final String key = (String) object;
            assertThat(key, is(oneOf("field2", "field3")));
        }

        {
            final IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> objectPath.evaluate("metadata.templates.template_3._arbitrary_key_")
            );
            assertThat(exception.getMessage(), equalTo("requested [_arbitrary_key_] but the map was empty"));
        }

        {
            final IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> objectPath.evaluate("metadata.templates.template_4._arbitrary_key_")
            );
            assertThat(exception.getMessage(), equalTo("requested meta-key [_arbitrary_key_] but the map unexpectedly contains this key"));
        }
    }

    public void testEvaluateStashInPropertyName() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startObject();
        xContentBuilder.startObject("field1");
        xContentBuilder.startObject("elements");
        xContentBuilder.field("element1", "value1");
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            xContentBuilder.contentType().xContent(),
            BytesReference.bytes(xContentBuilder)
        );
        try {
            objectPath.evaluate("field1.$placeholder.element1");
            fail("evaluate should have failed due to unresolved placeholder");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("stashed value not found for key [placeholder]"));
        }

        // Stashed value is whole property name
        Stash stash = new Stash();
        stash.stashValue("placeholder", "elements");
        Object object = objectPath.evaluate("field1.$placeholder.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stash key has dots
        Map<String, Object> stashedObject = new HashMap<>();
        stashedObject.put("subobject", "elements");
        stash.stashValue("object", stashedObject);
        object = objectPath.evaluate("field1.$object\\.subobject.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is part of property name
        stash.stashValue("placeholder", "ele");
        object = objectPath.evaluate("field1.${placeholder}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is inside of property name
        stash.stashValue("placeholder", "le");
        object = objectPath.evaluate("field1.e${placeholder}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Multiple stashed values in property name
        stash.stashValue("placeholder", "le");
        stash.stashValue("placeholder2", "nts");
        object = objectPath.evaluate("field1.e${placeholder}me${placeholder2}.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));

        // Stashed value is part of property name and has dots
        stashedObject.put("subobject", "ele");
        stash.stashValue("object", stashedObject);
        object = objectPath.evaluate("field1.${object\\.subobject}ments.element1", stash);
        assertThat(object, notNullValue());
        assertThat(object.toString(), equalTo("value1"));
    }

    @SuppressWarnings("unchecked")
    public void testEvaluateArrayAsRoot() throws Exception {
        XContentBuilder xContentBuilder = randomXContentBuilder();
        xContentBuilder.startArray();
        xContentBuilder.startObject();
        xContentBuilder.field("alias", "test_alias1");
        xContentBuilder.field("index", "test1");
        xContentBuilder.endObject();
        xContentBuilder.startObject();
        xContentBuilder.field("alias", "test_alias2");
        xContentBuilder.field("index", "test2");
        xContentBuilder.endObject();
        xContentBuilder.endArray();
        ObjectPath objectPath = ObjectPath.createFromXContent(
            XContentFactory.xContent(xContentBuilder.contentType()),
            BytesReference.bytes(xContentBuilder)
        );
        Object object = objectPath.evaluate("");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(List.class));
        assertThat(((List<Object>) object).size(), equalTo(2));
        object = objectPath.evaluate("0");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(Map.class));
        assertThat(((Map<String, Object>) object).get("alias"), equalTo("test_alias1"));
        object = objectPath.evaluate("1.index");
        assertThat(object, notNullValue());
        assertThat(object, instanceOf(String.class));
        assertThat(object, equalTo("test2"));
    }
}
