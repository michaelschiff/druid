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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.gazette.GazetteIndexTaskClientFactory;
import org.apache.druid.indexing.gazette.GazetteIndexTaskModule;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class GazetteSupervisorSpecTest
{
  private final ObjectMapper mapper;

  public GazetteSupervisorSpecTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(TaskStorage.class, null)
            .addValue(TaskMaster.class, null)
            .addValue(IndexerMetadataStorageCoordinator.class, null)
            .addValue(GazetteIndexTaskClientFactory.class, null)
            .addValue(ObjectMapper.class, mapper)
            .addValue(ServiceEmitter.class, new NoopServiceEmitter())
            .addValue(DruidMonitorSchedulerConfig.class, null)
            .addValue(RowIngestionMetersFactory.class, null)
            .addValue(SupervisorStateManagerConfig.class, null)
            .addValue(ExprMacroTable.class.getName(), LookupEnabledTestExprMacroTable.INSTANCE)
    );
    mapper.registerModules(new GazetteIndexTaskModule().getJacksonModules());
  }

  @Test
  public void testSerde() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"gazette\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"journalPrefix\": \"metrics/\",\n"
                  + "    \"brokerEndpoint\": \"localhost:8080\",\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    GazetteSupervisorSpec spec = mapper.readValue(json, GazetteSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics/", spec.getIoConfig().getJournalPrefix());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    GazetteSupervisorSpec spec2 = mapper.readValue(serialized, GazetteSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithInputFormat() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"gazette\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"timestampSpec\": {\n"
                  + "      \"column\": \"timestamp\",\n"
                  + "      \"format\": \"auto\"\n"
                  + "     },\n"
                  + "    \"dimensionsSpec\": {\n"
                  + "      \"dimensions\": [],\n"
                  + "      \"dimensionExclusions\": [\n"
                  + "        \"timestamp\",\n"
                  + "        \"value\"\n"
                  + "       ]\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"journalPrefix\": \"metrics/\",\n"
                  + "    \"inputFormat\": {\n"
                  + "      \"type\": \"json\",\n"
                  + "      \"flattenSpec\": {\n"
                  + "        \"useFieldDiscovery\": true,\n"
                  + "        \"fields\": []\n"
                  + "      },\n"
                  + "      \"featureSpec\": {}\n"
                  + "    },"
                  + "    \"brokerEndpoint\": \"localhost:8080\",\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    GazetteSupervisorSpec spec = mapper.readValue(json, GazetteSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics/", spec.getIoConfig().getJournalPrefix());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"inputFormat\":{"));

    GazetteSupervisorSpec spec2 = mapper.readValue(serialized, GazetteSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithSpec() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"gazette\",\n"
                  + "  \"spec\": {\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"journalPrefix\": \"metrics/\",\n"
                  + "    \"brokerEndpoint\": \"localhost:8080\",\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "  }\n"
                  + "}";
    GazetteSupervisorSpec spec = mapper.readValue(json, GazetteSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics/", spec.getIoConfig().getJournalPrefix());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"parser\":{"));

    GazetteSupervisorSpec spec2 = mapper.readValue(serialized, GazetteSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSerdeWithSpecAndInputFormat() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"gazette\",\n"
                  + "  \"spec\": {\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-kafka\",\n"
                  + "    \"timestampSpec\": {\n"
                  + "      \"column\": \"timestamp\",\n"
                  + "      \"format\": \"auto\"\n"
                  + "    },\n"
                  + "    \"dimensionsSpec\": {\n"
                  + "      \"dimensions\": [],\n"
                  + "      \"dimensionExclusions\": [\n"
                  + "        \"timestamp\",\n"
                  + "        \"value\"\n"
                  + "      ]\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"journalPrefix\": \"metrics/\",\n"
                  + "    \"brokerEndpoint\": \"localhost:8080\",\n"
                  + "    \"inputFormat\": {\n"
                  + "      \"type\": \"json\",\n"
                  + "      \"flattenSpec\": {\n"
                  + "        \"useFieldDiscovery\": true,\n"
                  + "        \"fields\": []\n"
                  + "      },\n"
                  + "      \"featureSpec\": {}\n"
                  + "    },"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "  }\n"
                  + "}";
    GazetteSupervisorSpec spec = mapper.readValue(json, GazetteSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics/", spec.getIoConfig().getJournalPrefix());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());
    String serialized = mapper.writeValueAsString(spec);

    // expect default values populated in reserialized string
    Assert.assertTrue(serialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(serialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(serialized.contains("\"suspended\":false"));
    Assert.assertTrue(serialized.contains("\"inputFormat\":{"));

    GazetteSupervisorSpec spec2 = mapper.readValue(serialized, GazetteSupervisorSpec.class);

    String stable = mapper.writeValueAsString(spec2);

    Assert.assertEquals(serialized, stable);
  }

  @Test
  public void testSuspendResume() throws IOException
  {
    String json = "{\n"
                  + "  \"type\": \"gazette\",\n"
                  + "  \"dataSchema\": {\n"
                  + "    \"dataSource\": \"metrics-gazette\",\n"
                  + "    \"parser\": {\n"
                  + "      \"type\": \"string\",\n"
                  + "      \"parseSpec\": {\n"
                  + "        \"format\": \"json\",\n"
                  + "        \"timestampSpec\": {\n"
                  + "          \"column\": \"timestamp\",\n"
                  + "          \"format\": \"auto\"\n"
                  + "        },\n"
                  + "        \"dimensionsSpec\": {\n"
                  + "          \"dimensions\": [],\n"
                  + "          \"dimensionExclusions\": [\n"
                  + "            \"timestamp\",\n"
                  + "            \"value\"\n"
                  + "          ]\n"
                  + "        }\n"
                  + "      }\n"
                  + "    },\n"
                  + "    \"metricsSpec\": [\n"
                  + "      {\n"
                  + "        \"name\": \"count\",\n"
                  + "        \"type\": \"count\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_sum\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleSum\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_min\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMin\"\n"
                  + "      },\n"
                  + "      {\n"
                  + "        \"name\": \"value_max\",\n"
                  + "        \"fieldName\": \"value\",\n"
                  + "        \"type\": \"doubleMax\"\n"
                  + "      }\n"
                  + "    ],\n"
                  + "    \"granularitySpec\": {\n"
                  + "      \"type\": \"uniform\",\n"
                  + "      \"segmentGranularity\": \"HOUR\",\n"
                  + "      \"queryGranularity\": \"NONE\"\n"
                  + "    }\n"
                  + "  },\n"
                  + "  \"ioConfig\": {\n"
                  + "    \"journalPrefix\": \"metrics/\",\n"
                  + "    \"brokerEndpoint\": \"localhost:8080\",\n"
                  + "    \"taskCount\": 1\n"
                  + "  }\n"
                  + "}";
    GazetteSupervisorSpec spec = mapper.readValue(json, GazetteSupervisorSpec.class);

    Assert.assertNotNull(spec);
    Assert.assertNotNull(spec.getDataSchema());
    Assert.assertEquals(4, spec.getDataSchema().getAggregators().length);
    Assert.assertNotNull(spec.getIoConfig());
    Assert.assertEquals("metrics/", spec.getIoConfig().getJournalPrefix());
    Assert.assertNotNull(spec.getTuningConfig());
    Assert.assertNull(spec.getContext());
    Assert.assertFalse(spec.isSuspended());

    String suspendedSerialized = mapper.writeValueAsString(spec.createSuspendedSpec());

    // expect default values populated in reserialized string
    Assert.assertTrue(suspendedSerialized.contains("\"tuningConfig\":{"));
    Assert.assertTrue(suspendedSerialized.contains("\"indexSpec\":{"));
    Assert.assertTrue(suspendedSerialized.contains("\"suspended\":true"));

    GazetteSupervisorSpec suspendedSpec = mapper.readValue(suspendedSerialized, GazetteSupervisorSpec.class);

    Assert.assertTrue(suspendedSpec.isSuspended());

    String runningSerialized = mapper.writeValueAsString(spec.createRunningSpec());

    GazetteSupervisorSpec runningSpec = mapper.readValue(runningSerialized, GazetteSupervisorSpec.class);

    Assert.assertFalse(runningSpec.isSuspended());
  }
}
