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
import org.zicat.tributary.sink.SinkGroupManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** SinkGroupManagerCollector. */
public class SinkGroupManagerCollector extends Collector {

    private final Map<String, List<SinkGroupManager>> sinkGroupManagerMap;
    private final List<String> labels = Arrays.asList("host", "id");
    private final String metricsIp;

    public SinkGroupManagerCollector(
            Map<String, List<SinkGroupManager>> sinkGroupManagerMap, String metricsIp) {
        this.sinkGroupManagerMap = sinkGroupManagerMap;
        this.metricsIp = metricsIp;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        final List<MetricFamilySamples> metricSamples = new ArrayList<>();
        metricSamples.add(collectionLag());
        return metricSamples;
    }

    /**
     * collection lag.
     *
     * @return GaugeMetricFamily
     */
    private MetricFamilySamples collectionLag() {
        final GaugeMetricFamily labeledGauge =
                new GaugeMetricFamily("sink_lag", "sink lag", labels);
        for (Map.Entry<String, List<SinkGroupManager>> entry : sinkGroupManagerMap.entrySet()) {
            final List<SinkGroupManager> sinkGroupManagers = entry.getValue();
            for (SinkGroupManager sinkGroupManager : sinkGroupManagers) {
                labeledGauge.addMetric(
                        Arrays.asList(metricsIp, sinkGroupManager.id()), sinkGroupManager.lag());
            }
        }
        return labeledGauge;
    }
}
