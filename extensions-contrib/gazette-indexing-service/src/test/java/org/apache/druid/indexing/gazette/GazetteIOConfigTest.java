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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class GazetteIOConfigTest
{
  private final ObjectMapper mapper;

  public GazetteIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new GazetteIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaultsAndSequenceNumbers() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"mytopic/part-000\":1, \"mytopic/part-001\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"mytopic/part-000\":15, \"mytopic/part-001\":200}},\n"
                     + "  \"brokerEndpoint\": \"localhost:9092\"\n"
                     + "}";

    GazetteIndexTaskIOConfig config = (GazetteIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of("mytopic/part-000", 1L, "mytopic/part-001", 10L), config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of("mytopic/part-000", 15L, "mytopic/part-001", 200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    //Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    Assert.assertFalse("maximumMessageTime", config.getMaximumMessageTime().isPresent());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\": \"start\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"mytopic/part-000\":1, \"mytopic/part-001\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\": \"end\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"mytopic/part-000\":15, \"mytopic/part-001\":200}},\n"
                     + "  \"brokerEndpoint\": \"localhost:8080\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    GazetteIndexTaskIOConfig config = (GazetteIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of("mytopic/part-000", 1L, "mytopic/part-001", 10L), config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of("mytopic/part-000", 15L, "mytopic/part-001", 200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    //Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertFalse(config.isUseTransaction());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    Assert.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime().get());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"brokerEndpoint\": \"localhost:8080\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("baseSequenceName"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"brokerEndpoint\": \"localhost:8080\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("startSequenceNumber"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testEndPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"brokerEndpoing\": \"localhost:8080\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endSequenceNumbers"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("brokerEndpoint"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndTopicMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"other\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"brokerEndpoint\": \"localhost:9092\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start topic/stream and end topic/stream must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndPartitionSetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15}},\n"
                     + "  \"brokerEndpoint\": \"localhost:9092\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start partition set and end partition set must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testEndOffsetGreaterThanStart() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":2}},\n"
                     + "  \"brokerEndpoint\": \"localhost:9092\",\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("end offset must be >= start offset"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

}
