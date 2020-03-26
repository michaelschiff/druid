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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.gazette.supervisor.GazetteSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskIOConfig;
import org.apache.druid.indexing.seekablestream.SeekableStreamStartSequenceNumbers;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class GazetteIndexTaskIOConfig extends SeekableStreamIndexTaskIOConfig<String, Long>
{
  private final long pollTimeout;
  private final String brokerEndpoint;

  @JsonCreator
  public GazetteIndexTaskIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startSequenceNumbers") SeekableStreamStartSequenceNumbers<String, Long> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") SeekableStreamEndSequenceNumbers<String, Long> endSequenceNumbers,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("brokerEndpoint") String brokerEndpoint,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime,
      @JsonProperty("inputFormat") @Nullable InputFormat inputFormat
  )
  {
    super(
        taskGroupId,
        baseSequenceName,
        startSequenceNumbers,
        endSequenceNumbers,
        useTransaction,
        minimumMessageTime,
        maximumMessageTime,
        inputFormat
    );

    this.brokerEndpoint = Preconditions.checkNotNull(brokerEndpoint, "brokerEndpoint");
    this.pollTimeout = pollTimeout != null ? pollTimeout : GazetteSupervisorIOConfig.DEFAULT_POLL_TIMEOUT_MILLIS;

    final SeekableStreamEndSequenceNumbers<String, Long> myEndSequenceNumbers = getEndSequenceNumbers();
    for (String journal : myEndSequenceNumbers.getPartitionSequenceNumberMap().keySet()) {
      Preconditions.checkArgument(
          myEndSequenceNumbers.getPartitionSequenceNumberMap()
                       .get(journal)
                       .compareTo(getStartSequenceNumbers().getPartitionSequenceNumberMap().get(journal)) >= 0,
          "end offset must be >= start offset for journal[%s]",
          journal
      );
    }
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public String getBrokerEndpoint()
  {
    return brokerEndpoint;
  }

  @Override
  public String toString()
  {
    return "KafkaIndexTaskIOConfig{" +
           "taskGroupId=" + getTaskGroupId() +
           ", baseSequenceName='" + getBaseSequenceName() + '\'' +
           ", startSequenceNumbers=" + getStartSequenceNumbers() +
           ", endSequenceNumbers=" + getEndSequenceNumbers() +
           ", pollTimeout=" + pollTimeout +
           ", brokerEndPoint=" + brokerEndpoint +
           ", useTransaction=" + isUseTransaction() +
           ", minimumMessageTime=" + getMinimumMessageTime() +
           ", maximumMessageTime=" + getMaximumMessageTime() +
           '}';
  }
}
