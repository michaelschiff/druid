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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.github.michaelschiff.gazette.Consumer;
import com.github.michaelschiff.gazette.TestJournalService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import dev.gazette.core.broker.protocol.JournalGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.SingleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.actions.TaskActionToolbox;
import org.apache.druid.indexing.common.actions.TaskAuditLogConfig;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.stats.RowIngestionMeters;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.stats.RowIngestionMetersTotals;
import org.apache.druid.indexing.common.task.IndexTaskTest;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.gazette.supervisor.GazetteSupervisor;
import org.apache.druid.indexing.gazette.supervisor.GazetteSupervisorIOConfig;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.MetadataTaskStorage;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTestBase;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.test.TestDataSegmentAnnouncer;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ListenableFutures;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class GazetteIndexTaskTest extends SeekableStreamIndexTaskTestBase
{
  private static final Logger log = new Logger(GazetteIndexTaskTest.class);
  private static final long POLL_RETRY_MS = 100;

  private static ServiceEmitter emitter;
  private static int topicPostfix;

  static {
    new GazetteIndexTaskModule().getJacksonModules().forEach(OBJECT_MAPPER::registerModule);
    OBJECT_MAPPER.registerSubtypes(new NamedType(TestableGazetteIndexTask.class, "index_gazette"));
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean logParseExceptions = true;
  private Integer maxParseExceptions = null;
  private Integer maxSavedParseExceptions = null;
  private boolean resetOffsetAutomatically = false;
  private boolean doHandoff = true;
  private Integer maxRowsPerSegment = null;
  private Long maxTotalRows = null;
  private Period intermediateHandoffPeriod = null;

  private AppenderatorsManager appenderatorsManager;
  private String journalPrefix;
  private final Set<Integer> checkpointRequestsHash = new HashSet<>();
  private RowIngestionMetersFactory rowIngestionMetersFactory;

  private static long[] partition0Offsets;
  private static long[] partition1Offsets;

  private static Map<String, List<ByteString>> generateRecords(String journalPrefix)
  {

    String[] partition0 = new String[] {
        jb("2008", "a", "y", "10", "20.0", "1.0") + "\n",
        jb("2009", "b", "y", "10", "20.0", "1.0") + "\n",
        jb("2010", "c", "y", "10", "20.0", "1.0") + "\n",
        jb("2011", "d", "y", "10", "20.0", "1.0") + "\n",

        jb("2011", "e", "y", "10", "20.0", "1.0") + "\n",
        jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0") + "\n",
        "unparseable" + "\n",
        "unparseable2" + "\n",
        "\n",

        jb("2013", "f", "y", "10", "20.0", "1.0") + "\n",
        jb("2049", "f", "y", "notanumber", "20.0", "1.0") + "\n",

        jb("2049", "f", "y", "10", "notanumber", "1.0") + "\n",
        jb("2049", "f", "y", "10", "20.0", "notanumber") + "\n",
        jb("2049", "f", "y", "10", "20.0", "2.0") + "\n",
    };

    partition0Offsets = new long[partition0.length];
    long soFar = 0;
    for (int i = 0; i < partition0Offsets.length; i++) {
      partition0Offsets[i] = soFar;
      if (i < partition0.length) {
        soFar += partition0[i].length();
      }
    }

    String[] partition1 = new String[]{
        jb("2012", "g", "y", "10", "20.0", "1.0") + "\n",
        jb("2011", "h", "y", "10", "20.0", "1.0") + "\n",
        jb("2010", "h", "y", "10", "20.0", "1.0") + "\n"
    };
    soFar = 0;
    partition1Offsets = new long[partition1.length];
    for (int i = 0; i < partition1Offsets.length; i++) {
      partition1Offsets[i] = soFar;
      if (i < partition1.length) {
        soFar += partition1[i].length();
      }
    }

    ByteString frag1 = ByteString.copyFromUtf8(String.join("", partition0[0], partition0[1], partition0[2], partition0[3]));
    ByteString frag2 = ByteString.copyFromUtf8(String.join("", partition0[4], partition0[5], partition0[6], partition0[7]));
    ByteString frag3 = ByteString.copyFromUtf8(String.join("", partition0[8], partition0[9]));
    ByteString frag4 = ByteString.copyFromUtf8(String.join("", partition0[10], partition0[11], partition0[12], partition0[13]));

    return ImmutableMap.of(
            journalPrefix + "0", ImmutableList.of(frag1, frag2, frag3, frag4),
            journalPrefix + "1", ImmutableList.of(ByteString.copyFromUtf8(String.join("", partition1[0], partition1[1], partition1[2]))));
  }

  private static Map<String, List<ByteString>> generateSinglePartitionRecords(String journalPrefix)
  {

    String[] partition0 = new String[] {
            jb("2008", "a", "y", "10", "20.0", "1.0") + "\n",
            jb("2009", "b", "y", "10", "20.0", "1.0") + "\n",
            jb("2010", "c", "y", "10", "20.0", "1.0") + "\n",
            jb("2011", "d", "y", "10", "20.0", "1.0") + "\n",
            jb("2011", "D", "y", "10", "20.0", "1.0") + "\n",
            jb("2012", "e", "y", "10", "20.0", "1.0") + "\n",
            jb("2009", "B", "y", "10", "20.0", "1.0") + "\n",
            jb("2008", "A", "x", "10", "20.0", "1.0") + "\n",
            jb("2009", "B", "x", "10", "20.0", "1.0") + "\n",
            jb("2010", "C", "x", "10", "20.0", "1.0") + "\n",
            jb("2011", "D", "x", "10", "20.0", "1.0") + "\n",
            jb("2011", "d", "x", "10", "20.0", "1.0") + "\n",
            jb("2012", "E", "x", "10", "20.0", "1.0") + "\n",
            jb("2009", "b", "x", "10", "20.0", "1.0") + "\n"
    };

    partition0Offsets = new long[partition0.length];
    long soFar = 0;
    for (int i = 0; i < partition0Offsets.length; i++) {
      partition0Offsets[i] = soFar;
      if (i < partition0.length) {
        soFar += partition0[i].length();
      }
    }
    ByteString frag1 = ByteString.copyFromUtf8(String.join("", partition0[0], partition0[1], partition0[2], partition0[3]));
    ByteString frag2 = ByteString.copyFromUtf8(String.join("", partition0[4], partition0[5], partition0[6], partition0[7]));
    ByteString frag3 = ByteString.copyFromUtf8(String.join("", partition0[8], partition0[9]));
    ByteString frag4 = ByteString.copyFromUtf8(String.join("", partition0[10], partition0[11], partition0[12], partition0[13]));

    return ImmutableMap.of(journalPrefix + "0", ImmutableList.of(frag1, frag2, frag3, frag4));
  }

  private static String getTopicName()
  {
    return "topic" + topicPostfix++ + "/";
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();

  public GazetteIndexTaskTest(LockGranularity lockGranularity)
  {
    super(lockGranularity);
  }

  private static TestJournalService journalService;
  private static JournalGrpc.JournalBlockingStub stub;

  @BeforeClass
  public static void setupClass() throws IOException
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new NoopEmitter()
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);

    journalService = new TestJournalService();
    String serverName = InProcessServerBuilder.generateName();
    Server server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(journalService)
            .build()
            .start();
    ManagedChannel channel = InProcessChannelBuilder.forName(serverName)
            .directExecutor()
            .build();
    stub = JournalGrpc.newBlockingStub(channel);

    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("gazette-task-test-%d")
        )
    );
  }

  @Before
  public void setupTest() throws IOException
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    logParseExceptions = true;
    maxParseExceptions = null;
    maxSavedParseExceptions = null;
    doHandoff = true;
    journalPrefix = getTopicName();
    reportsFile = File.createTempFile("GazetteIndexTaskTestReports-" + System.currentTimeMillis(), "json");
    appenderatorsManager = new TestAppenderatorsManager();
    makeToolboxFactory();
  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully(toolboxFactory.build(task).getConfig());
      }

      runningTasks.clear();
    }
    reportsFile.delete();
    destroyToolboxFactory();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    taskExec.shutdown();
    taskExec.awaitTermination(9999, TimeUnit.DAYS);

    emitter.close();
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    //Expect message 4 since org.apache.druid.indexing.gazette.IncrementalPublishingGazetteIndexTaskRunner.getNextStartOffset
    //returns the current sequence number (not +1 like kafka).  This is the same as the behavior for kinesis.
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInsertedWithLegacyParser() throws Exception
  {
    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        OLD_DATA_SCHEMA,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            null
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    //Expect message 4 since org.apache.druid.indexing.gazette.IncrementalPublishingGazetteIndexTaskRunner.getNextStartOffset
    //returns the current sequence number (not +1 like kafka).  This is the same as the behavior for kinesis.
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunBeforeDataInserted() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "locahost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    journalService.setEvents(data);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInsertedLiveReport() throws Exception
  {
    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[13])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    SeekableStreamIndexTaskRunner runner = task.getRunner();
    while (true) {
      Thread.sleep(1000);
      System.out.println(runner.getStatus());
      if (runner.getStatus() == Status.PUBLISHING) {
        break;
      }
    }
    Map rowStats = runner.doGetRowStats();
    Map totals = (Map) rowStats.get("totals");
    RowIngestionMetersTotals buildSegments = (RowIngestionMetersTotals) totals.get("buildSegments");

    Map movingAverages = (Map) rowStats.get("movingAverages");
    Map buildSegments2 = (Map) movingAverages.get("buildSegments");
    HashMap avg_1min = (HashMap) buildSegments2.get("1m");
    HashMap avg_5min = (HashMap) buildSegments2.get("5m");
    HashMap avg_15min = (HashMap) buildSegments2.get("15m");

    runner.resume();

    // Check metrics
    Assert.assertEquals(buildSegments.getProcessed(), task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(buildSegments.getUnparseable(), task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(buildSegments.getThrownAway(), task.getRunner().getRowIngestionMeters().getThrownAway());

    Assert.assertEquals(avg_1min.get("processed"), 0.0);
    Assert.assertEquals(avg_5min.get("processed"), 0.0);
    Assert.assertEquals(avg_15min.get("processed"), 0.0);
    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
  }

  @Test//(timeout = 60_000L)
  public void testIncrementalHandOff() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final SeekableStreamStartSequenceNumbers<String, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", 0L, journalPrefix + "1", 0L),
        ImmutableSet.of()
    );
    // Checkpointing will happen at either checkpoint1 or checkpoint2 depending on ordering
    // of events fetched across two partitions from Gazette
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[4], journalPrefix + "1", partition1Offsets[0])
    );
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[3], journalPrefix + "1", partition1Offsets[1])
    );
    final SeekableStreamEndSequenceNumbers<String, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[10], journalPrefix + "1", partition1Offsets[2])
    );
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertTrue(checkpoint1.getPartitionSequenceNumberMap().equals(currentOffsets)
                      || checkpoint2.getPartitionSequenceNumberMap()
                                    .equals(currentOffsets));
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new GazetteDataSourceMetadata(startPartitions)
            )
        )
    );

    // Check metrics
    Assert.assertEquals(8, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("d", "h")),
            sdd("2011/P1D", 1, ImmutableList.of("h"), ImmutableList.of("e")),
            sdd("2012/P1D", 0, ImmutableList.of("g")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[9], journalPrefix + "1", partition0Offsets[1]))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testIncrementalHandOffMaxTotalRows() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // incremental publish should happen every 3 records
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = 3L;

    // Insert data
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    Map<String, List<ByteString>> dataPart0 = ImmutableMap.of(journalPrefix + "0", data.get(journalPrefix + "0"), journalPrefix + "1", ImmutableList.of());
    Map<String, List<ByteString>> dataPart1 = ImmutableMap.of(journalPrefix + "1", data.get(journalPrefix + "1"));
    journalService.setEvents(dataPart0);

    final SeekableStreamStartSequenceNumbers<String, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", 0L, journalPrefix + "1", 0L),
        ImmutableSet.of()
    );
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint1 = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[3], journalPrefix + "1", partition1Offsets[0])
    );
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint2 = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[10], journalPrefix + "1", partition1Offsets[0])
    );

    final SeekableStreamEndSequenceNumbers<String, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[10], journalPrefix + "1", partition1Offsets[2])
    );
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());

    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);

    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }

    // add remaining records
    journalService.addEvents(dataPart1);
    final Map<String, Long> nextOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());


    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), nextOffsets);
    task.getRunner().setEndOffsets(nextOffsets, false);

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(2, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new GazetteDataSourceMetadata(startPartitions)
            )
        )
    );
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new GazetteDataSourceMetadata(
                    new SeekableStreamStartSequenceNumbers<>(journalPrefix, currentOffsets, ImmutableSet.of())
                )
            )
        )
    );

    // Check metrics
    Assert.assertEquals(8, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"), ImmutableList.of("d", "h")),
            sdd("2011/P1D", 1, ImmutableList.of("h"), ImmutableList.of("e")),
            sdd("2012/P1D", 0, ImmutableList.of("g")),
            sdd("2013/P1D", 0, ImmutableList.of("f"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[9], journalPrefix + "1", partition1Offsets[1]))
        ),
        newDataSchemaMetadata()
    );

    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[9], journalPrefix + "1", partition1Offsets[1]))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testTimeBasedIncrementalHandOff() throws Exception
  {
    final String baseSequenceName = "sequence0";
    // as soon as any segment hits maxRowsPerSegment or intermediateHandoffPeriod, incremental publishing should happen
    maxRowsPerSegment = Integer.MAX_VALUE;
    intermediateHandoffPeriod = new Period().withSeconds(0);

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final SeekableStreamStartSequenceNumbers<String, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[0], journalPrefix + "1", partition1Offsets[0]),
        ImmutableSet.of()
    );
    // Checkpointing will happen at checkpoint
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[1], journalPrefix + "1", partition1Offsets[0])
    );
    final SeekableStreamEndSequenceNumbers<String, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[2], journalPrefix + "1", partition1Offsets[0])
    );
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);

    // task will pause for checkpointing
    while (task.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    System.out.println(task.getRunner().getStatus());
    final Map<String, Long> currentOffsets = ImmutableMap.copyOf(task.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint.getPartitionSequenceNumberMap(), currentOffsets);
    task.getRunner().setEndOffsets(currentOffsets, false);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    Assert.assertEquals(1, checkpointRequestsHash.size());
    Assert.assertTrue(
        checkpointRequestsHash.contains(
            Objects.hash(
                NEW_DATA_SCHEMA.getDataSource(),
                0,
                new GazetteDataSourceMetadata(startPartitions)
            )
        )
    );

    // Check metrics
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[1], journalPrefix + "1", partition0Offsets[0]))
        ),
        newDataSchemaMetadata()
    );
  }

  DataSourceMetadata newDataSchemaMetadata()
  {
    return metadataStorageCoordinator.retrieveDataSourceMetadata(NEW_DATA_SCHEMA.getDataSource());
  }

  @Test//(timeout = 60_000L)
  public void testIncrementalHandOffReadsThroughEndOffsets() throws Exception
  {

    final String baseSequenceName = "sequence0";
    // as soon as any segment has more than one record, incremental publishing should happen
    maxRowsPerSegment = 2;

    journalService.setEvents(generateSinglePartitionRecords(journalPrefix));

    final SeekableStreamStartSequenceNumbers<String, Long> startPartitions =
        new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[0]), ImmutableSet.of());
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint1 =
        new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5]));
    final SeekableStreamEndSequenceNumbers<String, Long> checkpoint2 =
        new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[9]));
    final SeekableStreamEndSequenceNumbers<String, Long> endPartitions =
        new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", Long.MAX_VALUE));

    final GazetteIndexTask normalReplica = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final GazetteIndexTask staleReplica = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> normalReplicaFuture = runTask(normalReplica);
    // Simulating one replica is slower than the other
    final ListenableFuture<TaskStatus> staleReplicaFuture = ListenableFutures.transformAsync(
        taskExec.submit(() -> {
          Thread.sleep(1000);
          return staleReplica;
        }),
        this::runTask
    );

    while (normalReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    staleReplica.getRunner().pause();
    while (staleReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    Map<String, Long> currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint1.getPartitionSequenceNumberMap(), currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, false);
    staleReplica.getRunner().setEndOffsets(currentOffsets, false);

    while (normalReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    while (staleReplica.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    currentOffsets = ImmutableMap.copyOf(normalReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), currentOffsets);
    currentOffsets = ImmutableMap.copyOf(staleReplica.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint2.getPartitionSequenceNumberMap(), currentOffsets);

    normalReplica.getRunner().setEndOffsets(currentOffsets, true);
    staleReplica.getRunner().setEndOffsets(currentOffsets, true);

    Assert.assertEquals(TaskState.SUCCESS, normalReplicaFuture.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, staleReplicaFuture.get().getStatusCode());

    Assert.assertEquals(9, normalReplica.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, normalReplica.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, normalReplica.getRunner().getRowIngestionMeters().getThrownAway());

    Assert.assertEquals(9, staleReplica.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, staleReplica.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, staleReplica.getRunner().getRowIngestionMeters().getThrownAway());
  }

  @Test(timeout = 60_000L)
  public void testRunWithMinimumMessageTime() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            DateTimes.of("2010"),
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    journalService.setEvents(data);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithMaximumMessageTime() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            DateTimes.of("2010"),
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    journalService.setEvents(data);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(2, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0, ImmutableList.of("a")),
            sdd("2009/P1D", 0, ImmutableList.of("b")),
            sdd("2010/P1D", 0, ImmutableList.of("c"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithTransformSpec() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        NEW_DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                new SelectorDimFilter("dim1", "b", null),
                ImmutableList.of(
                    new ExpressionTransform("dim1t", "concat(dim1,dim1)", ExprMacroTable.nil())
                )
            )
        ),
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for the task to start reading
    while (task.getRunner().getStatus() != Status.READING) {
      Thread.sleep(10);
    }

    // Insert data
    journalService.setEvents(data);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    assertEqualsExceptVersion(ImmutableList.of(sdd("2009/P1D", 0)), publishedDescriptors);
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentColumn("dim1", publishedDescriptors.get(0)));
    Assert.assertEquals(ImmutableList.of("bb"), readSegmentColumn("dim1t", publishedDescriptors.get(0)));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // these will be ignored because reportParseExceptions is true
    maxParseExceptions = 1000;
    maxSavedParseExceptions = 2;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", Long.MAX_VALUE)),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, future.get().getStatusCode());

    // Check metrics
    // The first 5 messages parse fine
    Assert.assertEquals(5, task.getRunner().getRowIngestionMeters().getProcessed());
    // The 6th fails and we stop there because reportParseExceptions = true
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsSuccess() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 6;
    maxSavedParseExceptions = 6;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[13])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, status.getStatusCode());
    Assert.assertNull(status.getErrorMsg());

    // Check metrics
    Assert.assertEquals(4, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    assertEqualsExceptVersion(
        ImmutableList.of(sdd("2010/P1D", 0), sdd("2011/P1D", 0), sdd("2013/P1D", 0), sdd("2049/P1D", 0)),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[12]))),
        newDataSchemaMetadata()
    );

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 4,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 3,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> unparseableEvents = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        Arrays.asList(
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=20.0, met1=notanumber}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [Unable to parse value[notanumber] for field[met1],]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=10, dimFloat=notanumber, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to float,]",
            "Found unparseable columns in row: [MapBasedInputRow{timestamp=2049-01-01T00:00:00.000Z, event={timestamp=2049, dim1=f, dim2=y, dimLong=notanumber, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}], exceptions: [could not convert value [notanumber] to long,]",
            "Unable to parse row [unparseable2]",
            "Unable to parse row [unparseable]",
            "Encountered row with timestamp that cannot be represented as a long: [MapBasedInputRow{timestamp=246140482-04-24T15:36:27.903Z, event={timestamp=246140482-04-24T15:36:27.903Z, dim1=x, dim2=z, dimLong=10, dimFloat=20.0, met1=1.0}, dimensions=[dim1, dim1t, dim2, dimLong, dimFloat]}]"
        )
    );

    Assert.assertEquals(unparseableEvents, reportData.getUnparseableEvents());
  }

  @Test(timeout = 60_000L)
  public void testMultipleParseExceptionsFailure() throws Exception
  {
    reportParseExceptions = false;
    maxParseExceptions = 2;
    maxSavedParseExceptions = 2;

    // Insert data
    journalService.setEvents(generateRecords(journalPrefix));

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[10])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    TaskStatus status = future.get();

    // Wait for task to exit
    Assert.assertEquals(TaskState.FAILED, status.getStatusCode());
    IndexTaskTest.checkTaskStatusErrorMsgForParseExceptionsExceeded(status);

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getProcessedWithError());
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableList.of(), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    IngestionStatsAndErrorsTaskReportData reportData = getTaskReportData();

    Map<String, Object> expectedMetrics = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        ImmutableMap.of(
            RowIngestionMeters.PROCESSED, 3,
            RowIngestionMeters.PROCESSED_WITH_ERROR, 0,
            RowIngestionMeters.UNPARSEABLE, 3,
            RowIngestionMeters.THROWN_AWAY, 0
        )
    );
    Assert.assertEquals(expectedMetrics, reportData.getRowStats());

    Map<String, Object> unparseableEvents = ImmutableMap.of(
        RowIngestionMeters.BUILD_SEGMENTS,
        Arrays.asList(
            "Unable to parse row [unparseable2]",
            "Unable to parse row [unparseable]"
        )
    );

    Assert.assertEquals(unparseableEvents, reportData.getUnparseableEvents());
  }

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final GazetteIndexTask task2 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    journalService.setEvents(data);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final GazetteIndexTask task2 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[3]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[10])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "locahost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    // Insert data
    journalService.setEvents(data);

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.FAILED, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getUnparseable());

    //TODO(michaelschiff): confirm thrown away expectation in kafka test is just for null record which we dont have
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata, should all be from the first task
    final List<SegmentDescriptor> publishedDescriptors = publishedDescriptors();
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors
    );
    Assert.assertEquals(
            //The last record consumed by the task that got  to complete was number 4,
            // and since org.apache.druid.indexing.gazette.IncrementalPublishingGazetteIndexTaskRunner.getNextStartOffset
            // returns the current sequence number (same as kinesis (since +1 is not possible))
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {

    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            false,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final GazetteIndexTask task2 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[3]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[10])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            false,
            null,
            null,
            INPUT_FORMAT
        )
    );

    // Insert data
    journalService.setEvents(data);

    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc1 = sdd("2010/P1D", 0, ImmutableList.of("c"));
    SegmentDescriptorAndExpectedDim1Values desc2 = sdd("2011/P1D", 0, ImmutableList.of("d", "e"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(3, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc3 = sdd("2011/P1D", 1, ImmutableList.of("d", "e"));
    SegmentDescriptorAndExpectedDim1Values desc4 = sdd("2013/P1D", 0, ImmutableList.of("f"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc3, desc4), publishedDescriptors());
    Assert.assertNull(newDataSchemaMetadata());
  }

  @Test(timeout = 60_000L)
  public void testRunOneTaskTwoPartitions() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2], journalPrefix + "1", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5], journalPrefix + "1", partition0Offsets[2])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert data
    journalService.setEvents(data);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(5, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    SegmentDescriptorAndExpectedDim1Values desc1 = sdd("2010/P1D", 0, ImmutableList.of("c"));
    SegmentDescriptorAndExpectedDim1Values desc2 = sdd("2011/P1D", 0, ImmutableList.of("d", "e", "h"));
    // desc3 will not be created in GazetteIndexTask (0.12.x) as it does not create per Gazette partition Druid segments
    @SuppressWarnings("unused")
    SegmentDescriptor desc3 = sd("2011/P1D", 1);
    SegmentDescriptorAndExpectedDim1Values desc4 = sdd("2012/P1D", 0, ImmutableList.of("g"));
    assertEqualsExceptVersion(ImmutableList.of(desc1, desc2, desc4), publishedDescriptors());
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4], journalPrefix + "1", partition0Offsets[1]))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final GazetteIndexTask task2 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            1,
            "sequence1",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "1", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "1", partition0Offsets[1])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data
    journalService.setEvents(data);

    // Wait for tasks to exit
    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e")),
            sdd("2012/P1D", 0, ImmutableList.of("g"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4], journalPrefix + "1", 0L))
        ),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    List<ByteString> journal0Fragments = data.get(journalPrefix + "0");

    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[6])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    ByteString frag1 = journal0Fragments.get(0);
    ByteString frag2 = journal0Fragments.get(1);
    ByteString frag3 = journal0Fragments.get(2);

    Map<String, List<ByteString>> firstBatch = ImmutableMap.of(journalPrefix + "0",
            ImmutableList.of(frag1, frag2, frag3));
    journalService.setEvents(firstBatch);

    while (countEvents(task1) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Start a new task
    final GazetteIndexTask task2 = createTask(
        task1.getId(),
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[6])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert remaining data
    ByteString frag4 = journal0Fragments.get(3);

    List<ByteString> journal1Fragments = data.get(journalPrefix + "1");

    Map<String, List<ByteString>> rest = ImmutableMap.of(
            journalPrefix + "0", ImmutableList.of(frag4),
            journalPrefix + "1", journal1Fragments);
    journalService.addEvents(rest);

    // Wait for task to exit

    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(2, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(1, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRestoreAfterPersistingSequences() throws Exception
  {
    Map<String, List<ByteString>> data = generateSinglePartitionRecords(journalPrefix);
    List<ByteString> part0 = data.get(journalPrefix + "0");
    maxRowsPerSegment = 2;

    final GazetteIndexTask task1 = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[10])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final SeekableStreamStartSequenceNumbers<String, Long> checkpoint = new SeekableStreamStartSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]),
        ImmutableSet.of("0")
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish
    journalService.setEvents(ImmutableMap.of(journalPrefix + "0", ImmutableList.of(part0.get(0), part0.get(1))));

    while (task1.getRunner().getStatus() != Status.PAUSED) {
      Thread.sleep(10);
    }
    final Map<String, Long> currentOffsets = ImmutableMap.copyOf(task1.getRunner().getCurrentOffsets());
    Assert.assertEquals(checkpoint.getPartitionSequenceNumberMap(), currentOffsets);
    // Set endOffsets to persist sequences
    task1.getRunner().setEndOffsets(ImmutableMap.of("0", 5L), false);

    // Stop without publishing segment
    task1.stopGracefully(toolboxFactory.build(task1).getConfig());
    unlockAppenderatorBasePersistDirForTask(task1);

    Assert.assertEquals(TaskState.SUCCESS, future1.get().getStatusCode());

    // Start a new task
    final GazetteIndexTask task2 = createTask(
        task1.getId(),
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of("0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of("0", 10L)),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    // Wait for the task to start reading

    // Insert remaining data
    journalService.addEvents(ImmutableMap.of(journalPrefix + "0", ImmutableList.of(part0.get(2), part0.get(3))));

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(5, task1.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task1.getRunner().getRowIngestionMeters().getThrownAway());
    Assert.assertEquals(4, task2.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task2.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published segments & metadata
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2008/P1D", 0),
            sdd("2008/P1D", 1),
            sdd("2009/P1D", 0),
            sdd("2009/P1D", 1),
            sdd("2010/P1D", 0),
            sdd("2011/P1D", 0),
            sdd("2012/P1D", 0)
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of("0", 10L))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithPauseAndResume() throws Exception
  {
    Map<String, List<ByteString>> data = generateRecords(journalPrefix);
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[6])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Insert some data, but not enough for the task to finish
    List<ByteString> journal0Data = data.get(journalPrefix + "0");
    List<ByteString> journal0Frag0 = ImmutableList.of(journal0Data.get(0));
    List<ByteString> journal0Frag1 = ImmutableList.of(journal0Data.get(1));

    Map<String, List<ByteString>> firstBatch = ImmutableMap.of(journalPrefix + "0", journal0Frag0);
    journalService.setEvents(firstBatch);

    while (countEvents(task) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task));
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    Map<String, Long> currentOffsets = OBJECT_MAPPER.readValue(
        task.getRunner().pause().getEntity().toString(),
        new TypeReference<Map<String, Long>>()
        {
        }
    );
    Assert.assertEquals(Status.PAUSED, task.getRunner().getStatus());

    // Insert remaining data
    journalService.addEvents(ImmutableMap.of(journalPrefix + "0", journal0Frag1));

    try {
      future.get(10, TimeUnit.SECONDS);
      Assert.fail("Task completed when it should have been paused");
    }
    catch (TimeoutException e) {
      // carry on..
    }

    Assert.assertEquals(currentOffsets, task.getRunner().getCurrentOffsets());

    task.getRunner().resume();

    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
    //TODO(michaelschiff): this check doesn't work with the way we do getNextOffsets, need to understand
    // how this would *not* cause overcounting of the last message in an offset interval
    // See line 744 to 748 of {@link org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner}
    //Assert.assertEquals(task.getRunner().getEndOffsets(), task.getRunner().getCurrentOffsets());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(1, task.getRunner().getRowIngestionMeters().getUnparseable()); //5th record is not parseable
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
  {
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", Long.MAX_VALUE)),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(2000);
    }

    task.getRunner().pause();

    while (!task.getRunner().getStatus().equals(Status.PAUSED)) {
      Thread.sleep(25);
    }
  }

  @Test(timeout = 60_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndNextOffsetGreaterThanLeastAvailable() throws Exception
  {
    resetOffsetAutomatically = true;

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of("0", 200L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of("0", 500L)),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(20);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(Status.READING, task.getRunner().getStatus());
      // Offset should not be reset
      Assert.assertEquals(200L, (long) task.getRunner().getCurrentOffsets().get("0"));
    }
  }

  @Test(timeout = 60_000L)
  public void testRunContextSequenceAheadOfStartingOffsets() throws Exception
  {
    journalService.setEvents(generateRecords(journalPrefix));
    final TreeMap<Integer, Map<String, Long>> sequences = new TreeMap<>();
    // Here the sequence number is 1 meaning that one incremental handoff was done by the failed task
    // and this task should start reading from offset 2 for partition 0
    sequences.put(1, ImmutableMap.of(journalPrefix + "0", partition0Offsets[2]));
    final Map<String, Object> context = new HashMap<>();
    context.put(
        SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY,
        OBJECT_MAPPER.writerFor(GazetteSupervisor.CHECKPOINTS_TYPE_REF).writeValueAsString(sequences)
    );

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            // task should ignore these and use sequence info sent in the context
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 0L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[5])),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        ),
        context
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task.getRunner().getRowIngestionMeters().getProcessed());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getUnparseable());
    Assert.assertEquals(0, task.getRunner().getRowIngestionMeters().getThrownAway());

    // Check published metadata and segments in deep storage
    assertEqualsExceptVersion(
        ImmutableList.of(
            sdd("2010/P1D", 0, ImmutableList.of("c")),
            sdd("2011/P1D", 0, ImmutableList.of("d", "e"))
        ),
        publishedDescriptors()
    );
    Assert.assertEquals(
        new GazetteDataSourceMetadata(new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", partition0Offsets[4]))),
        newDataSchemaMetadata()
    );
  }

  @Test(timeout = 60_000L)
  public void testRunWithDuplicateRequest() throws Exception
  {
    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            "sequence0",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 200L), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of(journalPrefix + "0", 500L)),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    runTask(task);

    while (!task.getRunner().getStatus().equals(Status.READING)) {
      Thread.sleep(20);
    }

    // first setEndOffsets request
    task.getRunner().pause();
    task.getRunner().setEndOffsets(ImmutableMap.of(journalPrefix + "0", 500L), true);
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());

    // duplicate setEndOffsets request
    task.getRunner().pause();
    task.getRunner().setEndOffsets(ImmutableMap.of(journalPrefix + "0", 500L), true);
    Assert.assertEquals(Status.READING, task.getRunner().getStatus());
  }


  @Test(timeout = 60_000L)
  public void testCanStartFromLaterThanEarliestOffset() throws Exception
  {
    journalService.setEvents(generateRecords(journalPrefix));
    final String baseSequenceName = "sequence0";
    maxRowsPerSegment = Integer.MAX_VALUE;
    maxTotalRows = null;

    final SeekableStreamStartSequenceNumbers<String, Long> startPartitions = new SeekableStreamStartSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", 0L, journalPrefix + "1", partition0Offsets[1]),
        ImmutableSet.of()
    );

    final SeekableStreamEndSequenceNumbers<String, Long> endPartitions = new SeekableStreamEndSequenceNumbers<>(
        journalPrefix,
        ImmutableMap.of(journalPrefix + "0", partition0Offsets[10], journalPrefix + "1", partition0Offsets[2])
    );

    final GazetteIndexTask task = createTask(
        null,
        new GazetteIndexTaskIOConfig(
            0,
            baseSequenceName,
            startPartitions,
            endPartitions,
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );
    final ListenableFuture<TaskStatus> future = runTask(task);
    Assert.assertEquals(TaskState.SUCCESS, future.get().getStatusCode());
  }

  @Test
  public void testSerde() throws Exception
  {
    // This is both a serde test and a regression test for https://github.com/apache/druid/issues/7724.

    final GazetteIndexTask task = createTask(
        "taskid",
        NEW_DATA_SCHEMA.withTransformSpec(
            new TransformSpec(
                null,
                ImmutableList.of(new ExpressionTransform("beep", "nofunc()", ExprMacroTable.nil()))
            )
        ),
        new GazetteIndexTaskIOConfig(
            0,
            "sequence",
            new SeekableStreamStartSequenceNumbers<>(journalPrefix, ImmutableMap.of(), ImmutableSet.of()),
            new SeekableStreamEndSequenceNumbers<>(journalPrefix, ImmutableMap.of()),
            GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS,
            "localhost:8080",
            true,
            null,
            null,
            INPUT_FORMAT
        )
    );

    final Task task1 = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsBytes(task), Task.class);
    Assert.assertEquals(task, task1);
  }

  private List<ScanResultValue> scanData(final Task task, QuerySegmentSpec spec)
  {
    ScanQuery query = new Druids.ScanQueryBuilder().dataSource(
        NEW_DATA_SCHEMA.getDataSource()).intervals(spec).build();
    return task.getQueryRunner(query).run(QueryPlus.wrap(query)).toList();
  }

  private GazetteIndexTask createTask(
      final String taskId,
      final GazetteIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    return createTask(taskId, NEW_DATA_SCHEMA, ioConfig);
  }

  private GazetteIndexTask createTask(
      final String taskId,
      final GazetteIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  ) throws JsonProcessingException
  {
    return createTask(taskId, NEW_DATA_SCHEMA, ioConfig, context);
  }

  private GazetteIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final GazetteIndexTaskIOConfig ioConfig
  ) throws JsonProcessingException
  {
    final Map<String, Object> context = new HashMap<>();
    return createTask(taskId, dataSchema, ioConfig, context);
  }

  private GazetteIndexTask createTask(
      final String taskId,
      final DataSchema dataSchema,
      final GazetteIndexTaskIOConfig ioConfig,
      final Map<String, Object> context
  ) throws JsonProcessingException
  {
    final GazetteIndexTaskTuningConfig tuningConfig = new GazetteIndexTaskTuningConfig(
        1000,
        null,
        maxRowsPerSegment,
        maxTotalRows,
        new Period("P1Y"),
        null,
        null,
        null,
        null,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        null,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
    if (!context.containsKey(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY)) {
      final TreeMap<Integer, Map<String, Long>> checkpoints = new TreeMap<>();
      checkpoints.put(0, ioConfig.getStartSequenceNumbers().getPartitionSequenceNumberMap());
      final String checkpointsJson = OBJECT_MAPPER
          .writerFor(GazetteSupervisor.CHECKPOINTS_TYPE_REF)
          .writeValueAsString(checkpoints);
      context.put(SeekableStreamSupervisor.CHECKPOINTS_CTX_KEY, checkpointsJson);
    }

    final GazetteIndexTask task = new TestableGazetteIndexTask(
        taskId,
        null,
        cloneDataSchema(dataSchema),
        tuningConfig,
        ioConfig,
        context,
        null,
        null,
        rowIngestionMetersFactory,
        OBJECT_MAPPER,
        appenderatorsManager
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
  }

  private static DataSchema cloneDataSchema(final DataSchema dataSchema)
  {
    return new DataSchema(
        dataSchema.getDataSource(),
        dataSchema.getTimestampSpec(),
        dataSchema.getDimensionsSpec(),
        dataSchema.getAggregators(),
        dataSchema.getGranularitySpec(),
        dataSchema.getTransformSpec(),
        dataSchema.getParserMap(),
        OBJECT_MAPPER
    );
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesAndScanConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(),
                    new TimeseriesQueryEngine(),
                    (query, future) -> {
                      // do nothing
                    }
                )
            )
            .put(
                ScanQuery.class,
                new ScanQueryRunnerFactory(
                    new ScanQueryQueryToolChest(
                        new ScanQueryConfig(),
                        new DefaultGenericQueryMetricsFactory()
                    ),
                    new ScanQueryEngine(),
                    new ScanQueryConfig()
                )
            )
            .build()
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    rowIngestionMetersFactory = testUtils.getRowIngestionMetersFactory();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    for (Module module : new GazetteIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    objectMapper.registerSubtypes(new NamedType(TestableGazetteIndexTask.class, "index_gazette"));
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "baseDir").getPath(),
        new File(directory, "baseTaskDir").getPath(),
        null,
        50000,
        null,
        true,
        null,
        null,
        null
    );
    final TestDerbyConnector derbyConnector = derby.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createRulesTable();
    derbyConnector.createConfigTable();
    derbyConnector.createTaskTables();
    derbyConnector.createAuditTable();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        testUtils.getTestObjectMapper(),
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    taskLockbox = new TaskLockbox(taskStorage, metadataStorageCoordinator);
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        taskStorage,
        metadataStorageCoordinator,
        emitter,
        new SupervisorManager(null)
        {
          @Override
          public boolean checkPointDataSourceMetadata(
              String supervisorId,
              int taskGroupId,
              @Nullable DataSourceMetadata previousDataSourceMetadata
          )
          {
            log.info("Adding checkpoint hash to the set");
            checkpointRequestsHash.add(
                Objects.hash(
                    supervisorId,
                    taskGroupId,
                    previousDataSourceMetadata
                )
            );
            return true;
          }
        }
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox,
        new TaskAuditLogConfig(false)
    );
    final SegmentHandoffNotifierFactory handoffNotifierFactory = dataSource -> new SegmentHandoffNotifier()
    {
      @Override
      public boolean registerSegmentHandoffCallback(
          SegmentDescriptor descriptor,
          Executor exec,
          Runnable handOffRunnable
      )
      {
        if (doHandoff) {
          // Simulate immediate handoff
          exec.execute(handOffRunnable);
        }
        return true;
      }

      @Override
      public void start()
      {
        //Noop
      }

      @Override
      public void close()
      {
        //Noop
      }
    };
    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig);

    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        null, // taskExecutorNode
        taskActionClientFactory,
        emitter,
        dataSegmentPusher,
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        this::makeTimeseriesAndScanConglomerate,
        Execs.directExecutor(), // queryExecutorService
        NoopJoinableFactory.INSTANCE,
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(null, testUtils.getTestObjectMapper()),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        new CachePopulatorStats(),
        testUtils.getTestIndexMergerV9(),
        EasyMock.createNiceMock(DruidNodeAnnouncer.class),
        EasyMock.createNiceMock(DruidNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1, ServerType.INDEXER_EXECUTOR, 0),
        new SingleFileTaskReportFileWriter(reportsFile),
        null
    );
  }

  private static class TestableGazetteIndexTask extends GazetteIndexTask
  {

    @JsonCreator
    public TestableGazetteIndexTask(
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
            @JacksonInject AppenderatorsManager appenderatorsManager)
    {
      super(id, taskResource, dataSchema, tuningConfig, ioConfig, context, chatHandlerProvider, authorizerMapper, rowIngestionMetersFactory, configMapper, appenderatorsManager);
    }

    @Override
    protected GazetteRecordSupplier newTaskRecordSupplier()
    {
      return new GazetteRecordSupplier(new Consumer(stub));
    }
  }
}
