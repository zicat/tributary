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
import org.zicat.tributary.service.component.DynamicChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        metricSamples.add(collectionActiveSegment());
        metricSamples.add(collectionPageCacheUsage());
        metricSamples.add(collectionReadBytes());
        metricSamples.add(collectionWriteBytes());
        return metricSamples;
    }

    /**
     * collection write bytes.
     *
     * @return GaugeMetricFamily
     */
    private MetricFamilySamples collectionWriteBytes() {
        final GaugeMetricFamily labeledGauge =
                new GaugeMetricFamily("channel_write_bytes", "channel write bytes", labels);
        dynamicChannel
                .getChannels()
                .forEach(
                        (topic, channel) ->
                                labeledGauge.addMetric(
                                        Arrays.asList(topic, metricsIp), channel.writeBytes()));
        return labeledGauge;
    }

    /**
     * collection write bytes.
     *
     * @return GaugeMetricFamily
     */
    private MetricFamilySamples collectionReadBytes() {
        final GaugeMetricFamily labeledGauge =
                new GaugeMetricFamily("channel_read_bytes", "channel read bytes", labels);
        dynamicChannel
                .getChannels()
                .forEach(
                        (topic, channel) ->
                                labeledGauge.addMetric(
                                        Arrays.asList(topic, metricsIp), channel.readBytes()));
        return labeledGauge;
    }

    /**
     * collection page cache usage.
     *
     * @return GaugeMetricFamily
     */
    private MetricFamilySamples collectionPageCacheUsage() {
        final GaugeMetricFamily labeledGauge =
                new GaugeMetricFamily("channel_buffer_usage", "channel buffer usage", labels);
        dynamicChannel
                .getChannels()
                .forEach(
                        (topic, channel) ->
                                labeledGauge.addMetric(
                                        Arrays.asList(topic, metricsIp), channel.bufferUsage()));
        return labeledGauge;
    }

    /**
     * collection active segment.
     *
     * @return GaugeMetricFamily
     */
    private MetricFamilySamples collectionActiveSegment() {
        final GaugeMetricFamily labeledGauge =
                new GaugeMetricFamily(
                        "channel_active_segment", "channel total active segment", labels);
        dynamicChannel
                .getChannels()
                .forEach(
                        (topic, channel) ->
                                labeledGauge.addMetric(
                                        Arrays.asList(topic, metricsIp), channel.activeSegment()));
        return labeledGauge;
    }
}
