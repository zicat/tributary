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

package org.zicat.tributary.server.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** ChannelComponent. */
public class ChannelComponent extends AbstractComponent<String, Channel> {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelComponent.class);
    private static final List<String> LABELS = Arrays.asList("topic", "host");
    private final String metricsHost;

    public ChannelComponent(Map<String, Channel> elements, String metricsHost) {
        super(elements);
        this.metricsHost = metricsHost;
    }

    /**
     * for each channel.
     * @param action action
     */
    public void forEachChannel(BiConsumer<String, Channel> action) {
        elements.forEach(action);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return collect(
                new ElementHandler<Channel>() {
                    @Override
                    public List<String> additionalLabels(Channel channel) {
                        return LABELS;
                    }

                    @Override
                    public List<String> additionalLabelValues(Channel channel) {
                        return Arrays.asList(channel.topic(), metricsHost);
                    }
                });
    }

    public void flush() {
        for (Map.Entry<String, Channel> entry : elements.entrySet()) {
            try {
                entry.getValue().flush();
            } catch (Exception e) {
                LOG.warn("flush error", e);
            }
        }
    }

    public List<Channel> findChannels(String groupId) {
        final List<Channel> result = new ArrayList<>();
        for (Channel channel : elements.values()) {
            if (channel.groups().contains(groupId)) {
                result.add(channel);
            }
        }
        return result;
    }
}
