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

package org.zicat.tributary.sink.function;

import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.sink.CustomConfigBuilder;

/** ContextBuilder. */
public class ContextBuilder extends CustomConfigBuilder {

    private String id;
    private String topic;
    private String groupId;
    private int partitionId;
    private Offset startOffset;

    /**
     * set id.
     *
     * @param id id
     * @return this
     */
    public ContextBuilder id(String id) {
        this.id = id;
        return this;
    }

    /**
     * set topic.
     *
     * @param topic topic
     * @return this
     */
    public ContextBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    /**
     * set partition id.
     *
     * @param partitionId partitionId
     * @return this
     */
    public ContextBuilder partitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    /**
     * set start file offset.
     *
     * @param startOffset startOffset
     * @return this
     */
    public ContextBuilder startOffset(Offset startOffset) {
        this.startOffset = startOffset;
        return this;
    }

    public ContextBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    /**
     * build context.
     *
     * @return Context
     */
    public Context build() {
        if (groupId == null) {
            throw new IllegalArgumentException("groupId must not be null");
        }
        if (id == null) {
            throw new IllegalArgumentException("id must not be null");
        }
        if (topic == null) {
            throw new IllegalArgumentException("topic must not be null");
        }
        if (startOffset == null) {
            throw new IllegalArgumentException("startOffset must not be null");
        }
        return new Context(id, customConfig, topic, partitionId, groupId, startOffset);
    }

    /**
     * create builder.
     *
     * @return ContextBuilder
     */
    public static ContextBuilder newBuilder() {
        return new ContextBuilder();
    }
}
