/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.gazette;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GazetteCheckpointDataSourceMetadataSerdeTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testCheckPointDataSourceMetadataActionSerde() throws IOException
  {
    MAPPER.registerSubtypes(GazetteDataSourceMetadata.class);

    final GazetteDataSourceMetadata gazetteDataSourceMetadata =
        new GazetteDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic/",
                ImmutableMap.of("topic/part-000", 10L, "topic/part-001", 20L, "topic/part-002", 30L),
                ImmutableSet.of()
            )
        );
    final CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        null,
        gazetteDataSourceMetadata
    );

    final String serialized = MAPPER.writeValueAsString(checkpointAction);
    final CheckPointDataSourceMetadataAction deserialized = MAPPER.readValue(
        serialized,
        CheckPointDataSourceMetadataAction.class
    );
    Assert.assertEquals(checkpointAction, deserialized);
  }

  @Test
  public void testCheckPointDataSourceMetadataActionOldJsonSerde() throws IOException
  {
    MAPPER.registerSubtypes(GazetteDataSourceMetadata.class);
    final String jsonStr = "{\n" +
            "  \"type\": \"checkPointDataSourceMetadata\",\n" +
            "  \"supervisorId\": \"id_1\",\n" +
            "  \"taskGroupId\": 1,\n" +
            "  \"previousCheckPoint\": {\n" +
            "    \"type\": \"GazetteDataSourceMetadata\",\n" +
            "    \"partitions\": {\n" +
            "      \"type\": \"start\",\n" +
            "      \"stream\": \"topic/\",\n" +
            "      \"topic\": \"topic/\",\n" +
            "      \"partitionSequenceNumberMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"partitionOffsetMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"exclusivePartitions\": []\n" +
            "    }\n" +
            "  },\n" +
            "  \"checkpointMetadata\": {\n" +
            "    \"type\": \"GazetteDataSourceMetadata\",\n" +
            "    \"partitions\": {\n" +
            "      \"type\": \"start\",\n" +
            "      \"stream\": \"topic/\",\n" +
            "      \"topic\": \"topic/\",\n" +
            "      \"partitionSequenceNumberMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"partitionOffsetMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"exclusivePartitions\": []\n" +
            "    }\n" +
            "  },\n" +
            "  \"currentCheckPoint\": {\n" +
            "    \"type\": \"GazetteDataSourceMetadata\",\n" +
            "    \"partitions\": {\n" +
            "      \"type\": \"start\",\n" +
            "      \"stream\": \"topic/\",\n" +
            "      \"topic\": \"topic/\",\n" +
            "      \"partitionSequenceNumberMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"partitionOffsetMap\": {\n" +
            "        \"topic/part-000\": 10,\n" +
            "        \"topic/part-001\": 20,\n" +
            "        \"topic/part-002\": 30\n" +
            "      },\n" +
            "      \"exclusivePartitions\": []\n" +
            "    }\n" +
            "  },\n" +
            "  \"sequenceName\": \"dummy\"\n" +
            "}";

    final CheckPointDataSourceMetadataAction actual = MAPPER.readValue(
        jsonStr,
        CheckPointDataSourceMetadataAction.class
    );

    GazetteDataSourceMetadata kafkaDataSourceMetadata =
        new GazetteDataSourceMetadata(
            new SeekableStreamStartSequenceNumbers<>(
                "topic/",
                ImmutableMap.of("topic/part-000", 10L, "topic/part-001", 20L, "topic/part-002", 30L),
                ImmutableSet.of()
            )
        );
    CheckPointDataSourceMetadataAction checkpointAction = new CheckPointDataSourceMetadataAction(
        "id_1",
        1,
        kafkaDataSourceMetadata,
        kafkaDataSourceMetadata
    );
    Assert.assertEquals(checkpointAction, actual);
    System.out.println(MAPPER.writeValueAsString(actual));
  }
}
