/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.tributary.sink.config;

import org.zicat.tributary.common.config.DefaultReadableConfig;

import java.util.Map;

/** Context. */
public class Context extends DefaultReadableConfig {

    private final String id;
    private final String topic;
    private final int partitionId;
    private final String groupId;

    Context(
            String id,
            Map<String, Object> customConfig,
            String topic,
            int partitionId,
            final String groupId) {
        super(customConfig);
        this.id = id;
        this.topic = topic;
        this.partitionId = partitionId;
        this.groupId = groupId;
    }

    public String topic() {
        return topic;
    }

    public String groupId() {
        return groupId;
    }

    public int partitionId() {
        return partitionId;
    }

    public String id() {
        return id;
    }
}
