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

import io.prometheus.client.Collector;

import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import static org.zicat.tributary.common.util.Collections.concat;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.metric.MetricCollector;
import org.zicat.tributary.common.metric.MetricKey;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** AbstractComponent. */
public abstract class AbstractComponent<ID, ELEMENT extends Closeable & MetricCollector>
        extends Collector implements Component<ID, ELEMENT> {

    protected final AtomicBoolean close = new AtomicBoolean(false);
    protected final Map<ID, ELEMENT> elements;

    public AbstractComponent(Map<ID, ELEMENT> elements) {
        this.elements = Collections.unmodifiableMap(elements);
    }

    @Override
    public ELEMENT get(ID id) {
        return elements.get(id);
    }

    @Override
    public int size() {
        return elements.size();
    }

    @Override
    public void close() {
        if (close.compareAndSet(false, true)) {
            IOUtils.concurrentCloseQuietly(elements.values());
        }
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return collect(new ElementHandler<ELEMENT>() {});
    }

    /**
     * collect metric family samples.
     *
     * @return list
     */
    protected List<MetricFamilySamples> collect(ElementHandler<ELEMENT> elementHandler) {
        final List<MetricFamilySamples> metricSamples = new ArrayList<>();
        for (Map.Entry<ID, ELEMENT> entry : elements.entrySet()) {
            final ELEMENT element = entry.getValue();
            final List<String> labels = elementHandler.additionalLabels(element);
            final List<String> labelValues = elementHandler.additionalLabelValues(element);
            for (Map.Entry<MetricKey, Double> gaugeEntry : element.gaugeFamily().entrySet()) {
                final MetricKey metricKey = gaugeEntry.getKey();
                final double value = gaugeEntry.getValue();
                metricSamples.add(createGaugeMetricFamily(metricKey, labels, labelValues, value));
            }
            for (Map.Entry<MetricKey, Double> counterEntry : element.counterFamily().entrySet()) {
                final MetricKey metricKey = counterEntry.getKey();
                final double value = counterEntry.getValue();
                metricSamples.add(createCounterMetricFamily(metricKey, labels, labelValues, value));
            }
        }
        return metricSamples;
    }

    /**
     * ElementHandler.
     *
     * @param <ELEMENT> element type
     */
    public interface ElementHandler<ELEMENT> {

        /**
         * additionalLabels.
         *
         * @param element element
         * @return additionalLabels
         */
        default List<String> additionalLabels(ELEMENT element) {
            return Collections.emptyList();
        }

        /**
         * additionalLabelValues.
         *
         * @param element element
         * @return values
         */
        default List<String> additionalLabelValues(ELEMENT element) {
            return Collections.emptyList();
        }
    }

    /**
     * create gauge metric family.
     *
     * @param key key
     * @param additionalLabels additionalLabels
     * @param additionalLabelValues additionalLabelValues
     * @param value value
     * @return MetricFamilySamples
     */
    public static MetricFamilySamples createGaugeMetricFamily(
            MetricKey key,
            List<String> additionalLabels,
            List<String> additionalLabelValues,
            double value) {
        final List<String> labels = concat(key.getLabelNames(), additionalLabels);
        final List<String> labelValues = concat(key.getLabelValue(), additionalLabelValues);
        return new GaugeMetricFamily(key.getName(), key.getDescription(), labels)
                .addMetric(labelValues, value);
    }

    /**
     * create counter metric family.
     *
     * @param key key
     * @param additionalLabels additionalLabels
     * @param additionalLabelValues additionalLabelValues
     * @param value value
     * @return MetricFamilySamples
     */
    public static MetricFamilySamples createCounterMetricFamily(
            MetricKey key,
            List<String> additionalLabels,
            List<String> additionalLabelValues,
            double value) {
        final List<String> labels = concat(key.getLabelNames(), additionalLabels);
        final List<String> labelValues = concat(key.getLabelValue(), additionalLabelValues);
        return new CounterMetricFamily(key.getName(), key.getDescription(), labels)
                .addMetric(labelValues, value);
    }
}
