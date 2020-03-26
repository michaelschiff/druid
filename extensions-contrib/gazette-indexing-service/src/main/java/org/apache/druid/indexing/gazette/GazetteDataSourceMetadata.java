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
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamDataSourceMetadata;
import org.apache.druid.indexing.seekablestream.SeekableStreamEndSequenceNumbers;
import org.apache.druid.indexing.seekablestream.SeekableStreamSequenceNumbers;

public class GazetteDataSourceMetadata extends SeekableStreamDataSourceMetadata<String, Long>
{

  @JsonCreator
  public GazetteDataSourceMetadata(
         // naming this "partitions" feels weird when they are journals, but this needs to match
         // the value in the super class for proper deserializations
      @JsonProperty("partitions") SeekableStreamSequenceNumbers<String, Long> gazettePartitions
  )
  {
    super(gazettePartitions);
  }

  @Override
  public DataSourceMetadata asStartMetadata()
  {
    final SeekableStreamSequenceNumbers<String, Long> sequenceNumbers = getSeekableStreamSequenceNumbers();
    if (sequenceNumbers instanceof SeekableStreamEndSequenceNumbers) {
      return createConcreteDataSourceMetaData(
          ((SeekableStreamEndSequenceNumbers<String, Long>) sequenceNumbers).asStartPartitions(true)
      );
    } else {
      return this;
    }
  }

  @Override
  protected SeekableStreamDataSourceMetadata<String, Long> createConcreteDataSourceMetaData(
      SeekableStreamSequenceNumbers<String, Long> seekableStreamSequenceNumbers
  )
  {
    return new GazetteDataSourceMetadata(seekableStreamSequenceNumbers);
  }
}
