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

import org.zicat.tributary.source.base.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** SourceComponent. */
public class SourceComponent extends AbstractComponent<String, Source> {

    private final String metricsHost;
    private static final List<String> LABELS = Arrays.asList("id", "host");

    public SourceComponent(Map<String, Source> elements, String metricsHost) {
        super(elements);
        this.metricsHost = metricsHost;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return collect(new ElementHandler<Source>() {
            @Override
            public List<String> additionalLabels(Source source) {
                return LABELS;
            }

            @Override
            public List<String> additionalLabelValues(Source source) {
                return Arrays.asList(source.sourceId(), metricsHost);
            }
        });
    }
}
