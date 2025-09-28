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

package org.zicat.tributary.common;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** GaugeKey. */
public class MetricKey {

    private final String name;
    private final String description;
    private final List<String> labelNames;
    private final List<String> labelValue;

    public MetricKey(String name, String description) {
        this(name, description, null, null);
    }

    public MetricKey(String name) {
        this(name, name.replace("_", " "));
    }

    public MetricKey(
            String name, String description, List<String> labelNames, List<String> labelValue) {
        this.name = name;
        this.description = description;
        this.labelNames = labelNames == null ? java.util.Collections.emptyList() : labelNames;
        this.labelValue = labelValue == null ? new ArrayList<>() : labelValue;
    }

    public MetricKey addLabels(List<String> labelNames, List<String> labelValue) {
        return new MetricKey(
                name,
                description,
                Collections.concat(this.labelNames, labelNames),
                Collections.concat(this.labelValue, labelValue));
    }

    public MetricKey addLabel(String name, Object value) {
        return addLabels(singletonList(name), singletonList(String.valueOf(value)));
    }

    public final String getName() {
        return name;
    }

    public final String getDescription() {
        return description;
    }

    public final List<String> getLabelNames() {
        return labelNames;
    }

    public final List<String> getLabelValue() {
        return labelValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetricKey metricKey = (MetricKey) o;
        return Objects.equals(name, metricKey.name)
                && Objects.equals(description, metricKey.description)
                && Objects.equals(labelNames, metricKey.labelNames)
                && Objects.equals(labelValue, metricKey.labelValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, labelNames, labelValue);
    }
}
