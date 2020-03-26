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

import com.github.michaelschiff.JournalStubFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import dev.gazette.core.broker.protocol.JournalGrpc;
import dev.gazette.core.broker.protocol.Protocol;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;

import javax.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class GazetteRecordSupplier implements RecordSupplier<String, Long>
{
  private boolean closed;
  private JournalGrpc.JournalBlockingStub stub;
  private Map<StreamPartition<String>, Long> pollFrom = new HashMap<>();

  public GazetteRecordSupplier(
      String brokerEndpoint
  )
  {
    this(getJournalStub(brokerEndpoint));
  }

  @VisibleForTesting
  public GazetteRecordSupplier(
          JournalGrpc.JournalBlockingStub stub
  )
  {
    this.stub = stub;
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    for (StreamPartition<String> partition : streamPartitions) {
      pollFrom.put(partition, -1L);
    }
  }

  @Override
  public void seek(StreamPartition<String> partition, Long sequenceNumber)
  {
    pollFrom.put(partition, sequenceNumber);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      pollFrom.put(partition, 0L); //TODO(michaelschiff): this is likely incorrect
    }
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    for (StreamPartition<String> partition : partitions) {
      pollFrom.put(partition, -1L);
    }
  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    return pollFrom.keySet();
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, Long>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<String, Long>> polledRecords = new ArrayList<>();
    for (StreamPartition<String> partition : pollFrom.keySet()) {
      long offset = pollFrom.get(partition);
      Protocol.ReadRequest readRequest = Protocol.ReadRequest.newBuilder()
              .setJournal(partition.getPartitionId())
              .setOffset(offset)
              .build();
      Iterator<Protocol.ReadResponse> responseIter = stub.read(readRequest);
      while (responseIter.hasNext()) {
        Protocol.ReadResponse r = responseIter.next();
        ByteString content = r.getContent();
        if (!content.isEmpty()) {
          offset = r.getOffset();
          String[] recs = content.toStringUtf8().split("\n");
          List<byte[]> recBytes = new ArrayList<>();
          for (String rec : recs) {
            recBytes.add(rec.getBytes(StandardCharsets.UTF_8));
          }
          polledRecords.add(new OrderedPartitionableRecord<>(partition.getStream(), partition.getPartitionId(), offset, recBytes));
        }
      }
      pollFrom.put(partition, offset);
    }
    return polledRecords;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<String> partition)
  {
    Long currPos = getPosition(partition);
    seekToLatest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    Long currPos = getPosition(partition);
    seekToEarliest(Collections.singleton(partition));
    Long nextPos = getPosition(partition);
    seek(partition, currPos);
    return nextPos;
  }

  @Override
  public Long getPosition(StreamPartition<String> partition)
  {
    return pollFrom.get(partition);
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    Protocol.ListRequest listReq = Protocol.ListRequest.newBuilder()
            .setSelector(Protocol.LabelSelector.newBuilder()
                    .setInclude(Protocol.LabelSet.newBuilder().addLabels(
                            Protocol.Label.newBuilder()
                                    .setName("prefix")
                                    .setValue(stream)
                                    .build())
                            .build())
                    .build())
            .build();
    Set<String> res = new HashSet<>();
    Protocol.ListResponse list = stub.list(listReq);
    List<Protocol.ListResponse.Journal> journalsList = list.getJournalsList();
    for (Protocol.ListResponse.Journal journal : journalsList) {
      res.add(journal.getSpec().getName());
    }
    return res;
  }

  @Override
  public void close()
  {
    if (closed) {
      return;
    }
    closed = true;
  }

  private static JournalGrpc.JournalBlockingStub getJournalStub(String brokerEndpoint)
  {
    return JournalStubFactory.newBlockingStub(brokerEndpoint);
  }

  private static <T> T wrapExceptions(Callable<T> callable)
  {
    try {
      return callable.call();
    }
    catch (Exception e) {
      throw new StreamException(e);
    }
  }

  private static void wrapExceptions(Runnable runnable)
  {
    wrapExceptions(() -> {
      runnable.run();
      return null;
    });
  }
}
