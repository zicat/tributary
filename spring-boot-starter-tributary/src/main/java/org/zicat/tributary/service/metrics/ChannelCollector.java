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

package org.zicat.tributary.service.metrics;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.GaugeFamily;
import org.zicat.tributary.common.GaugeKey;
import org.zicat.tributary.service.component.DynamicChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** ChannelCollector metric collector. */
public class ChannelCollector extends Collector {

    private final DynamicChannel dynamicChannel;
    private final String metricsIp;
    private final List<String> labels = Arrays.asList("topic", "host");

    public ChannelCollector(DynamicChannel dynamicChannel, String metricsIp) {
        this.dynamicChannel = dynamicChannel;
        this.metricsIp = metricsIp;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        final List<MetricFamilySamples> metricSamples = new ArrayList<>();
        for (Channel channel : dynamicChannel.getChannels().values()) {
            final Map<GaugeKey, GaugeFamily> channelMetrics = channel.gaugeFamily();
            final String topic = channel.topic();
            for (GaugeFamily gaugeFamily : channelMetrics.values()) {
                final GaugeMetricFamily labeledGauge =
                        new GaugeMetricFamily(
                                gaugeFamily.getName(), gaugeFamily.getDescription(), labels);
                labeledGauge.addMetric(Arrays.asList(topic, metricsIp), gaugeFamily.getValue());
                metricSamples.add(labeledGauge);
            }
        }
        return metricSamples;
    }
}
