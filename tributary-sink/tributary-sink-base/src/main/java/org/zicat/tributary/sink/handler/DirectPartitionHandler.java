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

package org.zicat.tributary.sink.handler;

import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.records.RecordsIterator;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.sink.config.SinkGroupConfig;
import org.zicat.tributary.sink.function.Function;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * DirectPartitionHandler.
 *
 * <p>One Thread mode, One ${@link DirectPartitionHandler} instance bind with one {@link Function}
 * instance .
 */
public class DirectPartitionHandler extends PartitionHandler {

    private Function function;

    public DirectPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig config) {
        super(groupId, channel, partitionId, config);
    }

    @Override
    public void open() {
        this.function = createFunction(null);
    }

    @Override
    public void process(Offset offset, Iterator<byte[]> iterator) throws Exception {
        function.process(offset, RecordsIterator.wrap(iterator));
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.gaugeFamily());
        for (Map.Entry<MetricKey, Double> entry : function.gaugeFamily().entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        return base;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.counterFamily());
        for (Map.Entry<MetricKey, Double> entry : function.counterFamily().entrySet()) {
            base.merge(entry.getKey(), entry.getValue(), Double::sum);
        }
        return base;
    }

    @Override
    public Offset committableOffset() {
        final Offset commitableOffset = function.committableOffset();
        return commitableOffset == null ? startOffset : commitableOffset;
    }

    @Override
    public List<Function> getFunctions() {
        return Collections.singletonList(function);
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public void snapshot() throws Exception {
        function.snapshot();
    }

    @Override
    public void close() throws IOException {
        super.close(() -> IOUtils.closeQuietly(function));
    }
}
