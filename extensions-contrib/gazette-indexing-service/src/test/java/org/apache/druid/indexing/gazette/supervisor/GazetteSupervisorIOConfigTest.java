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

package org.apache.druid.indexing.gazette.supervisor;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.gazette.GazetteIndexTaskModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GazetteSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public GazetteSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new GazetteIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\",\n"
                     + "  \"journalPrefix\": \"test/\",\n"
                     + "  \"brokerEndpoint\": \"localhost:8080\"\n"
            + "}";

    GazetteSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                GazetteSupervisorIOConfig.class
            )
        ), GazetteSupervisorIOConfig.class
    );

    Assert.assertEquals("test/", config.getJournalPrefix());
    Assert.assertEquals(1, (int) config.getReplicas());
    Assert.assertEquals(1, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assert.assertEquals("localhost:8080", config.getBrokerEndpoint());
    Assert.assertEquals(100, config.getPollTimeout());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertEquals(false, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
    Assert.assertFalse("lateMessageRejectionPeriod", config.getLateMessageRejectionPeriod().isPresent());
    Assert.assertFalse("earlyMessageRejectionPeriod", config.getEarlyMessageRejectionPeriod().isPresent());
    Assert.assertFalse("lateMessageRejectionStartDateTime", config.getLateMessageRejectionStartDateTime().isPresent());
  }

  @Test
  public void testSerdeWithNonDefaultsWithLateMessagePeriod() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"gazette\",\n"
        + "  \"brokerEndpoint\": \"localhost:8080\",\n"
        + "  \"journalPrefix\": \"test/\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"pollTimeout\": 1000,\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\"\n"
        + "}";

    GazetteSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                GazetteSupervisorIOConfig.class
                )
            ), GazetteSupervisorIOConfig.class
        );

    Assert.assertEquals("test/", config.getJournalPrefix());
    Assert.assertEquals("localhost:8080", config.getBrokerEndpoint());
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(1000, config.getPollTimeout());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertEquals(true, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assert.assertEquals(Duration.standardHours(1), config.getLateMessageRejectionPeriod().get());
    Assert.assertEquals(Duration.standardHours(1), config.getEarlyMessageRejectionPeriod().get());
  }

  @Test
  public void testSerdeWithNonDefaultsWithLateMessageStartDateTime() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"gazette\",\n"
        + "  \"brokerEndpoint\": \"localhost:8080\",\n"
        + "  \"journalPrefix\": \"test/\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"pollTimeout\": 1000,\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"lateMessageRejectionStartDateTime\": \"2016-05-31T12:00Z\"\n"
        + "}";

    GazetteSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                GazetteSupervisorIOConfig.class
                )
            ), GazetteSupervisorIOConfig.class
        );

    Assert.assertEquals("test/", config.getJournalPrefix());
    Assert.assertEquals("localhost:8080", config.getBrokerEndpoint());
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(1000, config.getPollTimeout());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertEquals(true, config.isUseEarliestOffset());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getLateMessageRejectionStartDateTime().get());
  }

  @Test
  public void testJournalPrefixRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"gazette\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("journalPrefix"));
    mapper.readValue(jsonStr, GazetteSupervisorIOConfig.class);
  }

  @Test
  public void testBrokerEndpointRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"journalPrefix\": \"test/\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("brokerEndpoint"));
    mapper.readValue(jsonStr, GazetteSupervisorIOConfig.class);
  }

  @Test
  public void testSerdeWithBothExclusiveProperties() throws Exception
  {
    String jsonStr = "{\n"
        + "  \"type\": \"gazette\",\n"
        + "  \"journalPrefix\": \"test/\",\n"
        + "  \"brokerEndpoint\": \"localhost:8080\",\n"
        + "  \"replicas\": 3,\n"
        + "  \"taskCount\": 9,\n"
        + "  \"taskDuration\": \"PT30M\",\n"
        + "  \"pollTimeout\": 1000,\n"
        + "  \"startDelay\": \"PT1M\",\n"
        + "  \"period\": \"PT10S\",\n"
        + "  \"useEarliestOffset\": true,\n"
        + "  \"completionTimeout\": \"PT45M\",\n"
        + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"earlyMessageRejectionPeriod\": \"PT1H\",\n"
        + "  \"lateMessageRejectionStartDateTime\": \"2016-05-31T12:00Z\"\n"
        + "}";
    exception.expect(JsonMappingException.class);
    GazetteSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                GazetteSupervisorIOConfig.class
                )
            ), GazetteSupervisorIOConfig.class
        );
  }
}
