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

package org.zicat.tributary.sink.function;

import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.Clock;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.SystemClock;
import static org.zicat.tributary.common.records.RecordsUtils.sinkExtraHeaders;

import java.util.HashMap;
import java.util.Map;

/** AbstractFunction. */
public abstract class AbstractFunction implements Function {

    public static final ConfigOption<Clock> OPTION_SINK_CLOCK =
            ConfigOptions.key("_sink_clock").<Clock>objectType().defaultValue(new SystemClock());

    protected Context context;
    protected Clock clock;
    private Offset committableOffset;

    @Override
    public void open(Context context) throws Exception {
        this.context = context;
        this.clock = context.get(OPTION_SINK_CLOCK);
    }

    @Override
    public final Offset committableOffset() {
        return committableOffset;
    }

    /**
     * execute callback and persist offset.
     *
     * @param offset offset
     */
    public void commit(Offset offset) {
        this.committableOffset = offset;
    }

    @Override
    public String functionId() {
        return context.id();
    }

    @Override
    public String groupId() {
        return context.groupId();
    }

    @Override
    public String topic() {
        return context.topic();
    }

    @Override
    public int partitionId() {
        return context.partitionId();
    }

    /**
     * default sink extra headers.
     *
     * @return headers
     */
    public Map<String, byte[]> defaultSinkExtraHeaders() {
        final Map<String, byte[]> result = new HashMap<>();
        sinkExtraHeaders(clock, result);
        return result;
    }

    public Map<String, byte[]> defaultSinkExtraHeaders(Map<String, byte[]> result) {
        sinkExtraHeaders(clock, result);
        return result;
    }
}
