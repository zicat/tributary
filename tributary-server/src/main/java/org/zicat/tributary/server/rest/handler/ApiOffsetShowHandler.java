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

package org.zicat.tributary.server.rest.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import org.jetbrains.annotations.NotNull;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.server.component.ChannelComponent;
import static org.zicat.tributary.source.http.HttpMessageDecoder.http1_1JsonOkResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** ApiRestHandler. */
public class ApiOffsetShowHandler implements RestHandler {

    private final ChannelComponent component;

    public ApiOffsetShowHandler(ChannelComponent component) {
        this.component = component;
    }

    @Override
    public void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        final List<TopicGroupPartitionOffset> result = new ArrayList<>();
        component.forEachChannel((topic, channel) -> result.addAll(fromValues(topic, channel)));
        http1_1JsonOkResponse(ctx, MAPPER.writeValueAsBytes(result));
    }

    /**
     * from values.
     *
     * @param topic topic
     * @param channel channel
     * @return list
     */
    public static List<TopicGroupPartitionOffset> fromValues(String topic, Channel channel) {
        final List<TopicGroupPartitionOffset> result = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, Offset>> entry :
                channel.committedOffsets().entrySet()) {
            List<PartitionOffset> partitionOffsets = new ArrayList<>();
            for (Map.Entry<Integer, Offset> partitionOffsetEntry : entry.getValue().entrySet()) {
                partitionOffsets.add(
                        new PartitionOffset(
                                partitionOffsetEntry.getKey(), partitionOffsetEntry.getValue()));
            }
            partitionOffsets.sort(PartitionOffset::compareTo);
            result.add(new TopicGroupPartitionOffset(topic, entry.getKey(), partitionOffsets));
        }
        result.sort(TopicGroupPartitionOffset::compareTo);
        return result;
    }

    /** TopicGroupPartitionOffset. */
    public static class TopicGroupPartitionOffset implements Comparable<TopicGroupPartitionOffset> {
        private final String topic;
        private final String group;
        private final List<PartitionOffset> partitionOffsets;

        public TopicGroupPartitionOffset(
                String topic, String group, List<PartitionOffset> partitionOffsets) {
            this.topic = topic;
            this.group = group;
            this.partitionOffsets = partitionOffsets;
        }

        public String getTopic() {
            return topic;
        }

        public String getGroup() {
            return group;
        }

        public List<PartitionOffset> getPartitionOffsets() {
            return partitionOffsets;
        }

        @Override
        public int compareTo(@NotNull ApiOffsetShowHandler.TopicGroupPartitionOffset o) {
            final int topicCompareResult = getTopic().compareTo(o.getTopic());
            if (topicCompareResult != 0) {
                return topicCompareResult;
            }
            return getGroup().compareTo(o.getGroup());
        }
    }

    /** PartitionOffset. */
    public static class PartitionOffset implements Comparable<PartitionOffset> {
        private final Integer partition;
        private final Offset offset;

        public PartitionOffset(Integer partition, Offset offset) {
            this.partition = partition;
            this.offset = offset;
        }

        public Integer getPartition() {
            return partition;
        }

        public Offset getOffset() {
            return offset;
        }

        @Override
        public int compareTo(@NotNull ApiOffsetShowHandler.PartitionOffset o) {
            return getPartition().compareTo(o.getPartition());
        }
    }
}
