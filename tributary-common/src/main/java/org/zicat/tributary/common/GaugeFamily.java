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

import java.util.Objects;

/** GaugeFamily. */
public class GaugeFamily {

    private final String name;
    private final String description;
    private final double value;

    public GaugeFamily(String name, String description, double value) {
        this.name = name;
        this.description = description;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GaugeFamily that = (GaugeFamily) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    /**
     * merge .
     *
     * @param gaugeFamily gaugeFamily
     * @return GaugeFamily
     */
    public GaugeFamily merge(GaugeFamily gaugeFamily) {
        if (!equals(gaugeFamily)) {
            throw new IllegalArgumentException("gauge family merge fail, name diff");
        }
        return new GaugeFamily(name, description, this.value + gaugeFamily.getValue());
    }
}
