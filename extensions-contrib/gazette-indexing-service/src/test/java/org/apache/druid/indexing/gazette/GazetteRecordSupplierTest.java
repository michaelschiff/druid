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
import com.github.michaelschiff.gazette.Consumer;
import com.github.michaelschiff.gazette.TestJournalService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import dev.gazette.core.broker.protocol.JournalGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GazetteRecordSupplierTest
{
  private static String journalPrefix = "topic/";
  private static String journalPart0 = "part-000";
  private static String journalPart1 = "part-001";
  private static long poll_timeout_millis = 1000;
  private static int pollRetry = 5;
  private static int topicPosFix = 0;
  private static final ObjectMapper OBJECT_MAPPER = TestHelper.makeJsonMapper();

  private static JournalGrpc.JournalBlockingStub stub;
  private static TestJournalService journalService;

  private static Map<String, List<ByteString>> generateRecords()
  {
    ByteString frag1 = ByteString.copyFromUtf8(
            String.join("\n",
                    jb("2008", "a", "y", "10", "20.0", "1.0"),
                    jb("2009", "b", "y", "10", "20.0", "1.0"),
                    jb("2010", "c", "y", "10", "20.0", "1.0")));

    ByteString frag2 = ByteString.copyFromUtf8(
            String.join("\n",
                    jb("2011", "d", "y", "10", "20.0", "1.0"),
                    jb("2011", "e", "y", "10", "20.0", "1.0"),
                    jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0"),
                    "unparseable"));

    ByteString frag3 = ByteString.copyFromUtf8(
            String.join("\n",
                    "unparseable2",
                    jb("2013", "f", "y", "10", "20.0", "1.0")));

    ByteString frag4 = ByteString.copyFromUtf8(
            String.join("\n",
                    jb("2049", "f", "y", "notanumber", "20.0", "1.0"),
                    jb("2049", "f", "y", "10", "notanumber", "1.0"),
                    jb("2049", "f", "y", "10", "20.0", "notanumber"),
                    jb("2012", "g", "y", "10", "20.0", "1.0"),
                    jb("2011", "h", "y", "10", "20.0", "1.0")));

    return ImmutableMap.of(
            journalPrefix + journalPart0, ImmutableList.of(frag1, frag2, frag3, frag4),
                journalPrefix + journalPart1, ImmutableList.of());
  }

  private static String jb(String timestamp, String dim1, String dim2, String dimLong, String dimFloat, String met1)
  {
    try {
      return new ObjectMapper().writeValueAsString(
          ImmutableMap.builder()
                      .put("timestamp", timestamp)
                      .put("dim1", dim1)
                      .put("dim2", dim2)
                      .put("dimLong", dimLong)
                      .put("dimFloat", dimFloat)
                      .put("met1", met1)
                      .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getJournalPrefix()
  {
    return "topic-" + topicPosFix++ + "/";
  }

  //private List<OrderedPartitionableRecord<String, Long>> createOrderedPartitionableRecords()
  //{
    //List<OrderedPartitionableRecord<String, Long>> res = new ArrayList<>();
    //Map<String, Long> journalToOffset = new HashMap<>();
    //for (String journal : records.keySet()) {
      //for (byte[] rec : records.get(journal)) {
        //long offset = 0;
        //if (journalToOffset.containsKey(journal)) {
          //offset = journalToOffset.get(journal);
          //journalToOffset.put(journal, offset + 1);
        //} else {
          //journalToOffset.put(journal, 1L);
        //}
        //String[] split = journal.split("/");
        //res.add(new OrderedPartitionableRecord<>(
                //split[0],
                //split[1],
                //offset,
                //Collections.singletonList(rec)));
      //}
    //}
    //return res;
  //}

  @Before
  public void setupTest() throws IOException
  {
    journalPrefix = getJournalPrefix();
    journalService = new TestJournalService(generateRecords());
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
  }

  @Test
  public void testSupplierSetup()
  {

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(journalPrefix, journalPrefix + journalPart0),
        StreamPartition.of(journalPrefix, journalPrefix + journalPart1)
    );

    GazetteRecordSupplier recordSupplier = new GazetteRecordSupplier(new Consumer(stub));

    Assert.assertTrue(recordSupplier.getAssignment().isEmpty());

    recordSupplier.assign(partitions);

    Assert.assertEquals(partitions, recordSupplier.getAssignment());
    Assert.assertEquals(ImmutableSet.of(journalPrefix + journalPart0, journalPrefix + journalPart1), recordSupplier.getPartitionIds(journalPrefix));

    recordSupplier.close();
  }

  @Test
  public void testPoll() throws InterruptedException
  {
    StreamPartition<String> partition0 = StreamPartition.of(journalPrefix, journalPrefix + journalPart0);
    StreamPartition<String> partition1 = StreamPartition.of(journalPrefix, journalPrefix + journalPart1);

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(journalPrefix, journalPrefix + journalPart0),
        StreamPartition.of(journalPrefix, journalPrefix + journalPart1)
    );

    GazetteRecordSupplier recordSupplier = new GazetteRecordSupplier(new Consumer(stub));

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seek(partition0, 2L);
    recordSupplier.seek(partition1, 2L);

    //List<OrderedPartitionableRecord<String, Long>> initialRecords = createOrderedPartitionableRecords();

    List<OrderedPartitionableRecord<String, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);
    for (int i = 0; polledRecords.size() != 11 && i < pollRetry; i++) {
      polledRecords.addAll(recordSupplier.poll(poll_timeout_millis));
      Thread.sleep(200);
    }


    Assert.assertEquals(11, polledRecords.size());
    //Assert.assertTrue(initialRecords.containsAll(polledRecords));


    recordSupplier.close();

  }

  @Test
  public void testSeekToLatest()
  {
    StreamPartition<String> partition0 = StreamPartition.of(journalPrefix, journalPrefix + journalPart0);
    StreamPartition<String> partition1 = StreamPartition.of(journalPrefix, journalPrefix + journalPart1);

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(journalPrefix, journalPrefix + journalPart0),
        StreamPartition.of(journalPrefix, journalPrefix + journalPart1)
    );

    GazetteRecordSupplier recordSupplier = new GazetteRecordSupplier(new Consumer(stub));

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getEarliestSequenceNumber(partition1));

    recordSupplier.seekToLatest(partitions);
    List<OrderedPartitionableRecord<String, Long>> polledRecords = recordSupplier.poll(poll_timeout_millis);

    Assert.assertEquals(Collections.emptyList(), polledRecords);
    recordSupplier.close();
  }

  @Test
  public void testPosition()
  {
    StreamPartition<String> partition0 = StreamPartition.of(journalPrefix, journalPrefix + journalPart0);
    StreamPartition<String> partition1 = StreamPartition.of(journalPrefix, journalPrefix + journalPart1);

    Set<StreamPartition<String>> partitions = ImmutableSet.of(
        StreamPartition.of(journalPrefix, journalPrefix + journalPart0),
        StreamPartition.of(journalPrefix, journalPrefix + journalPart1)
    );

    GazetteRecordSupplier recordSupplier = new GazetteRecordSupplier(new Consumer(stub));

    recordSupplier.assign(partitions);
    recordSupplier.seekToEarliest(partitions);

    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition1));

    recordSupplier.seek(partition0, 4L);
    recordSupplier.seek(partition1, 5L);

    Assert.assertEquals(4L, (long) recordSupplier.getPosition(partition0));
    Assert.assertEquals(5L, (long) recordSupplier.getPosition(partition1));

    recordSupplier.seekToEarliest(Collections.singleton(partition0));
    Assert.assertEquals(0L, (long) recordSupplier.getPosition(partition0));

    //Gazette can seek to latest by setting next seek position to -1
    recordSupplier.seekToLatest(Collections.singleton(partition0));
    Assert.assertEquals(-1L, (long) recordSupplier.getPosition(partition0));

    long prevPos = recordSupplier.getPosition(partition0);
    recordSupplier.getEarliestSequenceNumber(partition0);
    Assert.assertEquals(prevPos, (long) recordSupplier.getPosition(partition0));

    recordSupplier.getLatestSequenceNumber(partition0);
    Assert.assertEquals(prevPos, (long) recordSupplier.getPosition(partition0));


    recordSupplier.close();
  }

}
