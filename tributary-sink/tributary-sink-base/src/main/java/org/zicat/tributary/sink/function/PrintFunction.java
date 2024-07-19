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

import io.prometheus.client.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.records.Records;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;

/** PrintFunction. */
public class PrintFunction extends AbstractFunction implements Trigger {

    protected static final Logger LOG = LoggerFactory.getLogger(PrintFunction.class);

    public static final ConfigOption<Duration> OPTION_IDLE_TRIGGER =
            ConfigOptions.key("idle.trigger")
                    .durationType()
                    .description("idle trigger, default 30s")
                    .defaultValue(Duration.ofSeconds(30));

    private static final Counter SINK_PRINT_COUNTER =
            Counter.build()
                    .name("sink_print_counter")
                    .help("sink print counter")
                    .labelNames("host", "id")
                    .register();

    private long triggerMillis;
    protected Counter.Child sinkCountChild;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        this.triggerMillis = context.get(OPTION_IDLE_TRIGGER).toMillis();
        this.sinkCountChild = labelHostId(SINK_PRINT_COUNTER);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        final StringBuilder sb =
                                new StringBuilder()
                                        .append(System.lineSeparator())
                                        .append("       Key: ")
                                        .append(new String(key, StandardCharsets.UTF_8))
                                        .append(System.lineSeparator())
                                        .append("       Value: ")
                                        .append(new String(value, StandardCharsets.UTF_8))
                                        .append(System.lineSeparator())
                                        .append("       Headers: [");
                        final Iterator<Map.Entry<String, byte[]>> it =
                                allHeaders.entrySet().iterator();
                        while (it.hasNext()) {
                            final Map.Entry<String, byte[]> entry = it.next();
                            sb.append(entry.getKey())
                                    .append(":")
                                    .append(new String(entry.getValue(), StandardCharsets.UTF_8));
                            if (it.hasNext()) {
                                sb.append(",");
                            }
                        }
                        sb.append("]");
                        LOG.info(sb.toString());
                    });
            sinkCountChild.inc(records.count());
        }
        commit(offset);
    }

    @Override
    public void close() {}

    @Override
    public long idleTimeMillis() {
        return triggerMillis;
    }

    @Override
    public void idleTrigger() {
        LOG.info("trigger idle period {}", triggerMillis);
    }
}
