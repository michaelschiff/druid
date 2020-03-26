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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.gazette.GazetteIndexTaskClientFactory;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorSpec;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;

import javax.annotation.Nullable;
import java.util.Map;

public class GazetteSupervisorSpec extends SeekableStreamSupervisorSpec
{
  private static final String TASK_TYPE = "gazette";

  @JsonCreator
  public GazetteSupervisorSpec(
      @JsonProperty("spec") @Nullable GazetteSupervisorIngestionSpec ingestionSchema,
      @JsonProperty("dataSchema") @Nullable DataSchema dataSchema,
      @JsonProperty("tuningConfig") @Nullable GazetteSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") @Nullable GazetteSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("suspended") Boolean suspended,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject GazetteIndexTaskClientFactory gazetteIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject SupervisorStateManagerConfig supervisorStateManagerConfig
  )
  {
    super(
        ingestionSchema != null
        ? ingestionSchema
        : new GazetteSupervisorIngestionSpec(
            dataSchema,
            ioConfig,
            tuningConfig != null
            ? tuningConfig
            : GazetteSupervisorTuningConfig.defaultConfig()
        ),
        context,
        suspended,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        gazetteIndexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  public String getType()
  {
    return TASK_TYPE;
  }

  @Override
  public String getSource()
  {
    return getIoConfig() != null ? getIoConfig().getJournalPrefix() : null;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new GazetteSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (GazetteIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        this,
        rowIngestionMetersFactory
    );
  }

  @Override
  @Deprecated
  @JsonProperty
  public GazetteSupervisorTuningConfig getTuningConfig()
  {
    return (GazetteSupervisorTuningConfig) super.getTuningConfig();
  }

  @Override
  @Deprecated
  @JsonProperty
  public GazetteSupervisorIOConfig getIoConfig()
  {
    return (GazetteSupervisorIOConfig) super.getIoConfig();
  }

  @Override
  @JsonProperty
  public GazetteSupervisorIngestionSpec getSpec()
  {
    return (GazetteSupervisorIngestionSpec) super.getSpec();
  }

  @Override
  protected GazetteSupervisorSpec toggleSuspend(boolean suspend)
  {
    return new GazetteSupervisorSpec(
        getSpec(),
        getDataSchema(),
        getTuningConfig(),
        getIoConfig(),
        getContext(),
        suspend,
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        (GazetteIndexTaskClientFactory) indexTaskClientFactory,
        mapper,
        emitter,
        monitorSchedulerConfig,
        rowIngestionMetersFactory,
        supervisorStateManagerConfig
    );
  }

  @Override
  public String toString()
  {
    return "GazetteSupervisorSpec{" +
           "dataSchema=" + getDataSchema() +
           ", tuningConfig=" + getTuningConfig() +
           ", ioConfig=" + getIoConfig() +
           ", context=" + getContext() +
           ", suspend=" + isSuspended() +
           '}';
  }
}
