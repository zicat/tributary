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

import io.prometheus.client.SimpleCollector;

import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.sink.utils.HostUtils;

/** AbstractFunction. */
public abstract class AbstractFunction implements Function {

    public static final ConfigOption<String> OPTION_METRICS_HOST =
            ConfigOptions.key("metricsHost")
                    .stringType()
                    .description(
                            "export the dimension value of metrics, default the first network card ip")
                    .defaultValue(HostUtils.getLocalHostString(".*"));

    protected Context context;
    private Offset committableOffset;
    private String metricsHost;

    @Override
    public void open(Context context) throws Exception {
        this.context = context;
        this.committableOffset = context.startOffset();
        this.metricsHost = context.get(OPTION_METRICS_HOST);
    }

    @Override
    public final Offset committableOffset() {
        return committableOffset;
    }

    /**
     * get metrics host.
     *
     * @return string
     */
    public final String metricsHost() {
        return metricsHost;
    }

    /**
     * get context.
     *
     * @return context
     */
    public final Context context() {
        return context;
    }

    /**
     * execute callback and persist offset.
     *
     * @param newCommittableOffset newCommittableOffset
     */
    public void commit(Offset newCommittableOffset) {
        if (newCommittableOffset == null) {
            return;
        }
        this.committableOffset = newCommittableOffset;
    }

    /**
     * label host group id topic.
     *
     * @param collector collector
     * @return child
     * @param <CHILD> CHILD
     */
    protected <CHILD> CHILD labelHostId(SimpleCollector<CHILD> collector) {
        return labelHostId(context, collector);
    }

    /**
     * label host group id topic.
     *
     * @param context context
     * @param collector collector
     * @return child
     * @param <CHILD> CHILD
     */
    public static <CHILD> CHILD labelHostId(Context context, SimpleCollector<CHILD> collector) {
        return collector.labels(context.get(OPTION_METRICS_HOST), context.id());
    }

    /** default snapshot, subclass can override this function. */
    @Override
    public void snapshot() throws Exception {}
}
