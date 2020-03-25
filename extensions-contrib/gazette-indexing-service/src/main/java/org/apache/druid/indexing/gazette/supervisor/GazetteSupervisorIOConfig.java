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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Map;

public class GazetteSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  public static final long DEFAULT_POLL_TIMEOUT_MILLIS = 100;

  private final String brokerEndpoint;
  private final long pollTimeout;


  @JsonCreator
  public GazetteSupervisorIOConfig(
      @JsonProperty("brokerEndpoint") String brokerEndpoint,
      @JsonProperty("journalPrefix") String journalPrefix,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("pollTimeout") Long pollTimeout,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime
  )
  {
    super(
        Preconditions.checkNotNull(journalPrefix, "journalPrefix"),
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestOffset,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        lateMessageRejectionStartDateTime
    );

    this.brokerEndpoint = Preconditions.checkNotNull(brokerEndpoint, "brokerEndpoint");
    this.pollTimeout = pollTimeout != null ? pollTimeout : DEFAULT_POLL_TIMEOUT_MILLIS;
  }

  @JsonProperty
  public String getJournalPrefix()
  {
    return getStream();
  }

  @JsonProperty
  public long getPollTimeout()
  {
    return pollTimeout;
  }

  @JsonProperty
  public boolean isUseEarliestOffset()
  {
    return isUseEarliestSequenceNumber();
  }



  @Override
  public String toString()
  {
    return "GazetteSupervisorIOConfig{" +
           "journalPrefix='" + getJournalPrefix() + '\'' +
           ", brokerEndpoint=" + brokerEndpoint +
           ", replicas=" + getReplicas() +
           ", taskCount=" + getTaskCount() +
           ", taskDuration=" + getTaskDuration() +
           ", pollTimeout=" + pollTimeout +
           ", startDelay=" + getStartDelay() +
           ", period=" + getPeriod() +
           ", useEarliestOffset=" + isUseEarliestOffset() +
           ", completionTimeout=" + getCompletionTimeout() +
           ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
           ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
           ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
           '}';
  }

}
