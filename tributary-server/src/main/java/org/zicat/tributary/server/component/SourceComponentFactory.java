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

import static org.zicat.tributary.common.ReadableConfig.DEFAULT_KEY_HANDLER;
import static org.zicat.tributary.common.SpiFactory.findFactory;

import io.prometheus.client.GaugeMetricFamily;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.common.*;
import org.zicat.tributary.source.base.Source;
import org.zicat.tributary.source.base.SourceFactory;

import java.util.*;

/** SourceComponentFactory. */
public class SourceComponentFactory implements SafeFactory<SourceComponent> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceComponentFactory.class);
    private static final String SPLIT = ".";

    public static final ConfigOption<String> OPTION_IMPLEMENT =
            ConfigOptions.key("implement")
                    .stringType()
                    .description("source implement")
                    .noDefaultValue();

    public static final ConfigOption<String> OPTION_CHANNEL =
            ConfigOptions.key("channel").stringType().description("channel name").noDefaultValue();

    private final ReadableConfig sourceConfig;
    private final ChannelComponent channelComponent;
    private final String metricsHost;

    public SourceComponentFactory(
            ReadableConfig sourceConfig, ChannelComponent channelComponent, String metricsHost) {
        this.sourceConfig = sourceConfig;
        this.channelComponent = channelComponent;
        this.metricsHost = metricsHost;
    }

    @Override
    public SourceComponent create() {
        final Map<String, Source> sources = new HashMap<>();
        final Set<String> sourceIds = sourceConfig.groupKeys(DEFAULT_KEY_HANDLER);
        try {
            for (String sourceId : sourceIds) {
                final String head = sourceId + SPLIT;
                final String topic = sourceConfig.get(OPTION_CHANNEL.concatHead(head));
                final Channel channel = channelComponent.get(topic);
                if (channel == null) {
                    throw new TributaryRuntimeException("channel." + topic + " not defined");
                }
                final String implementId = sourceConfig.get(OPTION_IMPLEMENT.concatHead(head));
                final SourceFactory sourceFactory = findFactory(implementId, SourceFactory.class);
                final ReadableConfig config = sourceConfig.filterAndRemovePrefixKey(head);
                final Source source = sourceFactory.createSource(sourceId, channel, config);
                source.start();
                sources.put(sourceId, source);
            }
            LOG.info("create source success, sources {}", sources.keySet());
            return new DefaultSourceComponent(sources, metricsHost);
        } catch (Exception e) {
            sources.forEach((k, v) -> IOUtils.closeQuietly(v));
            throw new TributaryRuntimeException(e);
        }
    }

    /** DefaultSourceComponent. */
    private static class DefaultSourceComponent extends SourceComponent {

        private final List<String> labels = Arrays.asList("id", "host");
        private final String metricsHost;

        private DefaultSourceComponent(Map<String, Source> sources, String metricsHost) {
            super(sources);
            this.metricsHost = metricsHost;
        }

        @Override
        public List<MetricFamilySamples> collect() {
            final List<MetricFamilySamples> metricSamples = new ArrayList<>();
            for (Source source : elements.values()) {
                final String id = source.sourceId();
                final List<String> labelsValue = Arrays.asList(id, metricsHost);
                for (GaugeFamily gaugeFamily : source.gaugeFamily().values()) {
                    final String name = gaugeFamily.getName();
                    final String desc = gaugeFamily.getDescription();
                    metricSamples.add(
                            new GaugeMetricFamily(name, desc, labels)
                                    .addMetric(labelsValue, gaugeFamily.getValue()));
                }
            }
            return metricSamples;
        }
    }
}
