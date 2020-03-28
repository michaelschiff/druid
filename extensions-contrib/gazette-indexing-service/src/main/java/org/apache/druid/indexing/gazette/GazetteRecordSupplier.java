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

import com.github.michaelschiff.gazette.Consumer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import dev.gazette.core.broker.protocol.JournalGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.common.StreamPartition;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class GazetteRecordSupplier implements RecordSupplier<String, Long>
{
  private boolean closed;
  private Consumer consumer;

  Map<String, String> prefixes = new HashMap<>();

  public GazetteRecordSupplier(
      String brokerEndpoint
  )
  {
    this(new Consumer(getJournalStub(brokerEndpoint)));
  }

  @VisibleForTesting
  public GazetteRecordSupplier(
      Consumer consumer
  )
  {
    this.consumer = consumer;
  }

  @Override
  public void assign(Set<StreamPartition<String>> streamPartitions)
  {
    Set<String> journals = new HashSet<>();
    for (StreamPartition<String> partition : streamPartitions) {
      prefixes.put(partition.getPartitionId(), partition.getStream());
      journals.add(partition.getPartitionId());
    }
    consumer.assign(journals);
  }

  @Override
  public void seek(StreamPartition<String> partition, Long sequenceNumber)
  {
    consumer.seek(partition.getPartitionId(), sequenceNumber);
  }

  @Override
  public void seekToEarliest(Set<StreamPartition<String>> partitions)
  {
    consumer.seekToEarliest(partitions.stream().map(p -> p.getPartitionId()).collect(Collectors.toSet()));
  }

  @Override
  public void seekToLatest(Set<StreamPartition<String>> partitions)
  {
    consumer.seekToLatest(partitions.stream().map(p -> p.getPartitionId()).collect(Collectors.toSet()));

  }

  @Override
  public Set<StreamPartition<String>> getAssignment()
  {
    return consumer.getAssignment()
            .stream()
            .map(j -> new StreamPartition<>(prefixes.get(j), j))
            .collect(Collectors.toSet());
  }

  @Nonnull
  @Override
  public List<OrderedPartitionableRecord<String, Long>> poll(long timeout)
  {
    List<OrderedPartitionableRecord<String, Long>> res = new ArrayList<>();
    for (Consumer.Record record : consumer.poll()) {
      res.add(new OrderedPartitionableRecord<>(
              prefixes.get(record.getJournal()),
              record.getJournal(),
              record.getOffset(),
              ImmutableList.of(record.getData())
      ));
    }
    return res;
  }

  @Override
  public Long getLatestSequenceNumber(StreamPartition<String> partition)
  {
    return consumer.getWriteHead(partition.getPartitionId());
  }

  @Override
  public Long getEarliestSequenceNumber(StreamPartition<String> partition)
  {
    return consumer.getLowestValidOffset(partition.getPartitionId());
  }

  @Override
  public Long getPosition(StreamPartition<String> partition)
  {
    return consumer.getPosition(partition.getPartitionId());
  }

  @Override
  public Set<String> getPartitionIds(String stream)
  {
    return consumer.listJournalsForPrefix(stream);
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
    ManagedChannel channel = ManagedChannelBuilder.forTarget(brokerEndpoint).build();
    return JournalGrpc.newBlockingStub(channel);
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
