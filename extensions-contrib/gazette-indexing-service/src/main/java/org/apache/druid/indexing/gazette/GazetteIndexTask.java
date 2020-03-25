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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class GazetteIndexTask extends SeekableStreamIndexTask<String, Long>
{
  private static final String TYPE = "index_kafka";

  private final GazetteIndexTaskIOConfig ioConfig;
  private final ObjectMapper configMapper;

  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public GazetteIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") GazetteIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") GazetteIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject ObjectMapper configMapper,
      @JacksonInject AppenderatorsManager appenderatorsManager
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        chatHandlerProvider,
        authorizerMapper,
        rowIngestionMetersFactory,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE),
        appenderatorsManager
    );
    this.configMapper = configMapper;
    this.ioConfig = ioConfig;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );
  }

  long getPollRetryMs()
  {
    return pollRetryMs;
  }


//TODO(michaelschiff): gazette grpc bindings instead
//  @Deprecated
//  KafkaConsumer<byte[], byte[]> newConsumer()
//  {
//    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
//    try {
//      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
//
//      final Map<String, Object> consumerConfigs = KafkaConsumerConfigs.getConsumerProperties();
//      final Properties props = new Properties();
//      KafkaRecordSupplier.addConsumerPropertiesFromConfig(
//          props,
//          configMapper,
//          ioConfig.getConsumerProperties()
//      );
//      props.putAll(consumerConfigs);
//
//      return new KafkaConsumer<>(props);
//    }
//    finally {
//      Thread.currentThread().setContextClassLoader(currCtxCl);
//    }
//  }
//
//  @Deprecated
//  static void assignPartitions(
//      final KafkaConsumer consumer,
//      final String topic,
//      final Set<Integer> partitions
//  )
//  {
//    consumer.assign(
//        new ArrayList<>(
//            partitions.stream().map(n -> new TopicPartition(topic, n)).collect(Collectors.toList())
//        )
//    );
//  }

  @Override
  protected SeekableStreamIndexTaskRunner<String, Long> createTaskRunner()
  {
    //noinspection unchecked
    return new IncrementalPublishingGazetteIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        chatHandlerProvider,
        savedParseExceptions,
        rowIngestionMetersFactory,
        appenderatorsManager,
        lockGranularityToUse
    );
  }

  @Override
  protected GazetteRecordSupplier newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      //TODO(michaelschiff): maybe there is stuff we want to configure here
      //final Map<String, Object> props = new HashMap<>(((GazetteIndexTaskIOConfig) super.ioConfig).getConsumerProperties());
      //props.put("auto.offset.reset", "none");
      return new GazetteRecordSupplier(configMapper);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  @JsonProperty
  public GazetteIndexTaskTuningConfig getTuningConfig()
  {
    return (GazetteIndexTaskTuningConfig) super.getTuningConfig();
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  @JsonProperty("ioConfig")
  public GazetteIndexTaskIOConfig getIOConfig()
  {
    return (GazetteIndexTaskIOConfig) super.getIOConfig();
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
