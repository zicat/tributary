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

import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.MetricCollector;
import org.zicat.tributary.common.MetricKey;
import org.zicat.tributary.server.component.SinkComponent.SinkGroupManagerList;
import org.zicat.tributary.sink.SinkGroupManager;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** SinkComponent. */
public class SinkComponent extends AbstractComponent<String, SinkGroupManagerList> {

    private static final List<String> LABELS = Arrays.asList("topic", "groupId", "host");

    private final int size;

    public SinkComponent(Map<String, SinkGroupManagerList> elements) {
        super(elements);
        this.size = elements.values().stream().mapToInt(List::size).sum();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        final List<MetricFamilySamples> metricSamples = new ArrayList<>();
        for (Map.Entry<String, SinkGroupManagerList> entry : elements.entrySet()) {
            final SinkGroupManagerList sinkGroupManagers = entry.getValue();
            for (Map.Entry<MetricKey, Double> gaugeEntry :
                    sinkGroupManagers.gaugeFamily().entrySet()) {
                final MetricKey metricKey = gaugeEntry.getKey();
                final double value = gaugeEntry.getValue();
                metricSamples.add(createGaugeMetricFamily(metricKey, value));
            }
            for (Map.Entry<MetricKey, Double> counterEntry :
                    sinkGroupManagers.counterFamily().entrySet()) {
                final MetricKey metricKey = counterEntry.getKey();
                final double value = counterEntry.getValue();
                metricSamples.add(createCounterMetricFamily(metricKey, value));
            }
        }
        return metricSamples;
    }

    /** SinkGroupManagerList. */
    public static class SinkGroupManagerList extends ArrayList<SinkGroupManager>
            implements Closeable, MetricCollector {

        private final String groupId;
        private final String metricsHost;

        public SinkGroupManagerList(String groupId, String metricsHost) {
            this.groupId = groupId;
            this.metricsHost = metricsHost;
        }

        @Override
        public void close() {
            this.forEach(IOUtils::closeQuietly);
        }

        @Override
        public Map<MetricKey, Double> gaugeFamily() {
            final Map<MetricKey, Double> result = new HashMap<>();
            for (SinkGroupManager sinkGroupManager : this) {
                final List<String> labelValues =
                        Arrays.asList(sinkGroupManager.topic(), groupId, metricsHost);
                for (Map.Entry<MetricKey, Double> gaugeEntry :
                        sinkGroupManager.gaugeFamily().entrySet()) {
                    final MetricKey metricKey = gaugeEntry.getKey();
                    final double value = gaugeEntry.getValue();
                    result.merge(metricKey.addLabels(LABELS, labelValues), value, Double::sum);
                }
            }
            return result;
        }

        @Override
        public Map<MetricKey, Double> counterFamily() {
            final Map<MetricKey, Double> result = new HashMap<>();
            for (SinkGroupManager sinkGroupManager : this) {
                final List<String> labelValues =
                        Arrays.asList(sinkGroupManager.topic(), groupId, metricsHost);
                for (Map.Entry<MetricKey, Double> counterEntry :
                        sinkGroupManager.counterFamily().entrySet()) {
                    final MetricKey metricKey = counterEntry.getKey();
                    final double value = counterEntry.getValue();
                    result.merge(metricKey.addLabels(LABELS, labelValues), value, Double::sum);
                }
            }
            return result;
        }
    }
}
