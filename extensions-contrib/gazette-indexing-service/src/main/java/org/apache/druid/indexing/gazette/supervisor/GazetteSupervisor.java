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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.utils.RandomIdUtils;
import org.apache.druid.indexing.gazette.GazetteDataSourceMetadata;
import org.apache.druid.indexing.gazette.GazetteIndexTask;
import org.apache.druid.indexing.gazette.GazetteIndexTaskClientFactory;
import org.apache.druid.indexing.gazette.GazetteIndexTaskIOConfig;
import org.apache.druid.indexing.gazette.GazetteIndexTaskTuningConfig;
import org.apache.druid.indexing.gazette.GazetteRecordSupplier;
import org.apache.druid.indexing.gazette.GazetteSequenceNumber;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorReportPayload;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Supervisor responsible for managing the GazetteIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link GazetteSupervisorSpec} which includes the Gazette journal selectors and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Gazette Journals that match the selectors
 * and the list of running indexing tasks and ensures that all journals are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Gazette offsets.
 */
public class GazetteSupervisor extends SeekableStreamSupervisor<String, Long>
{
  public static final TypeReference<TreeMap<Integer, Map<Integer, Long>>> CHECKPOINTS_TYPE_REF =
      new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
      {
      };

  private static final EmittingLogger log = new EmittingLogger(GazetteSupervisor.class);
  private static final Long NOT_SET = -1L;
  private static final Long END_OF_PARTITION = Long.MAX_VALUE;

  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private volatile Map<String, Long> latestSequenceFromStream;


  private final GazetteSupervisorSpec spec;

  public GazetteSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final GazetteIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final GazetteSupervisorSpec spec,
      final RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    super(
        StringUtils.format("GazetteSupervisor-%s", spec.getDataSchema().getDataSource()),
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        mapper,
        spec,
        rowIngestionMetersFactory,
        false
    );

    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();
  }


  @Override
  protected RecordSupplier<String, Long> setupRecordSupplier()
  {
    return new GazetteRecordSupplier(
            spec.getIoConfig().getBrokerEndpoint()
    );
  }

  @Override
  protected int getTaskGroupIdForPartition(String partitionId)
  {
    return Math.abs(partitionId.hashCode()) % spec.getIoConfig().getTaskCount();
  }

  @Override
  protected boolean checkSourceMetadataMatch(DataSourceMetadata metadata)
  {
    return metadata instanceof GazetteDataSourceMetadata;
  }

  @Override
  protected boolean doesTaskTypeMatchSupervisor(Task task)
  {
    return task instanceof GazetteIndexTask;
  }

  @Override
  protected SeekableStreamSupervisorReportPayload<String, Long> createReportPayload(
      int numPartitions,
      boolean includeOffsets
  )
  {
    GazetteSupervisorIOConfig ioConfig = spec.getIoConfig();
    Map<String, Long> partitionLag = getRecordLagPerPartition(getHighestCurrentOffsets());
    return new GazetteSupervisorReportPayload(
        spec.getDataSchema().getDataSource(),
        ioConfig.getJournalPrefix(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        includeOffsets ? latestSequenceFromStream : null,
        includeOffsets ? partitionLag : null,
        includeOffsets ? partitionLag.values().stream().mapToLong(x -> Math.max(x, 0)).sum() : null,
        includeOffsets ? sequenceLastUpdated : null,
        spec.isSuspended(),
        stateManager.isHealthy(),
        stateManager.getSupervisorState().getBasicState(),
        stateManager.getSupervisorState(),
        stateManager.getExceptionEvents()
    );
  }


  @Override
  protected SeekableStreamIndexTaskIOConfig createTaskIoConfig(
      int groupId,
      Map<String, Long> startPartitions,
      Map<String, Long> endPartitions,
      String baseSequenceName,
      DateTime minimumMessageTime,
      DateTime maximumMessageTime,
      Set<String> exclusiveStartSequenceNumberPartitions,
      SeekableStreamSupervisorIOConfig ioConfig
  )
  {
    GazetteSupervisorIOConfig gazetteIoConfig = (GazetteSupervisorIOConfig) ioConfig;
    return new GazetteIndexTaskIOConfig(
        groupId,
        baseSequenceName,
        new SeekableStreamStartSequenceNumbers<>(gazetteIoConfig.getJournalPrefix(), startPartitions, Collections.emptySet()),
        new SeekableStreamEndSequenceNumbers<>(gazetteIoConfig.getJournalPrefix(), endPartitions),
            gazetteIoConfig.getPollTimeout(),
        ((GazetteSupervisorIOConfig) ioConfig).getBrokerEndpoint(),
        true,
        minimumMessageTime,
        maximumMessageTime,
        ioConfig.getInputFormat(
            spec.getDataSchema().getParser() == null ? null : spec.getDataSchema().getParser().getParseSpec()
        )
    );
  }

  @Override
  protected List<SeekableStreamIndexTask<String, Long>> createIndexTasks(
      int replicas,
      String baseSequenceName,
      ObjectMapper sortingMapper,
      TreeMap<Integer, Map<String, Long>> sequenceOffsets,
      SeekableStreamIndexTaskIOConfig taskIoConfig,
      SeekableStreamIndexTaskTuningConfig taskTuningConfig,
      RowIngestionMetersFactory rowIngestionMetersFactory
  ) throws JsonProcessingException
  {
    final String checkpoints = sortingMapper.writerFor(CHECKPOINTS_TYPE_REF).writeValueAsString(sequenceOffsets);
    final Map<String, Object> context = createBaseTaskContexts();
    context.put(CHECKPOINTS_CTX_KEY, checkpoints);

    List<SeekableStreamIndexTask<String, Long>> taskList = new ArrayList<>();
    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(baseSequenceName, RandomIdUtils.getRandomId());
      taskList.add(new GazetteIndexTask(
          taskId,
          new TaskResource(baseSequenceName, 1),
          spec.getDataSchema(),
          (GazetteIndexTaskTuningConfig) taskTuningConfig,
          (GazetteIndexTaskIOConfig) taskIoConfig,
          context,
          null,
          null,
          rowIngestionMetersFactory,
          sortingMapper,
          null
      ));
    }
    return taskList;
  }

  @Override
  protected Map<String, Long> getPartitionRecordLag()
  {
    Map<String, Long> highestCurrentOffsets = getHighestCurrentOffsets();

    if (latestSequenceFromStream == null) {
      return null;
    }

    if (!latestSequenceFromStream.keySet().equals(highestCurrentOffsets.keySet())) {
      log.warn(
          "Lag metric: Kafka partitions %s do not match task partitions %s",
          latestSequenceFromStream.keySet(),
          highestCurrentOffsets.keySet()
      );
    }

    return getRecordLagPerPartition(highestCurrentOffsets);
  }

  @Nullable
  @Override
  protected Map<String, Long> getPartitionTimeLag()
  {
    // time lag not currently support with kafka
    return null;
  }

  @Override
  // suppress use of CollectionUtils.mapValues() since the valueMapper function is dependent on map key here
  @SuppressWarnings("SSBasedInspection")
  protected Map<String, Long> getRecordLagPerPartition(Map<String, Long> currentOffsets)
  {
    return currentOffsets
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Entry::getKey,
                e -> latestSequenceFromStream != null
                     && latestSequenceFromStream.get(e.getKey()) != null
                     && e.getValue() != null
                     ? latestSequenceFromStream.get(e.getKey()) - e.getValue()
                     : Integer.MIN_VALUE
            )
        );
  }

  @Override
  protected Map<String, Long> getTimeLagPerPartition(Map<String, Long> currentOffsets)
  {
    return null;
  }

  @Override
  protected GazetteDataSourceMetadata createDataSourceMetaDataForReset(String topic, Map<String, Long> map)
  {
    return new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(topic, map));
  }

  @Override
  protected OrderedSequenceNumber<Long> makeSequenceNumber(Long seq, boolean isExclusive)
  {
    return GazetteSequenceNumber.of(seq);
  }

  @Override
  protected Long getNotSetMarker()
  {
    return NOT_SET;
  }

  @Override
  protected Long getEndOfPartitionMarker()
  {
    return END_OF_PARTITION;
  }

  @Override
  protected boolean isEndOfShard(Long seqNum)
  {
    return false;
  }

  @Override
  protected boolean isShardExpirationMarker(Long seqNum)
  {
    return false;
  }

  @Override
  protected boolean useExclusiveStartSequenceNumberForNonFirstSequence()
  {
    return false;
  }

  @Override
  protected void updateLatestSequenceFromStream(
      RecordSupplier<String, Long> recordSupplier,
      Set<StreamPartition<String>> journals
  )
  {
    latestSequenceFromStream = journals.stream()
                                         .collect(Collectors.toMap(
                                             StreamPartition::getPartitionId,
                                             recordSupplier::getPosition
                                         ));
  }

  @Override
  protected String baseTaskName()
  {
    return "index_gazette";
  }

  @Override
  @VisibleForTesting
  public GazetteSupervisorIOConfig getIoConfig()
  {
    return spec.getIoConfig();
  }

  @VisibleForTesting
  public GazetteSupervisorTuningConfig getTuningConfig()
  {
    return spec.getTuningConfig();
  }
}
