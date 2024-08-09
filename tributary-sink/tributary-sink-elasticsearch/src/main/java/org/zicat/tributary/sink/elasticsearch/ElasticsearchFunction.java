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

package org.zicat.tributary.sink.elasticsearch;

import io.prometheus.client.Counter;
import org.elasticsearch.client.RestHighLevelClient;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;

import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory.createRestClientBuilder;

/** ElasticsearchFunction. */
@SuppressWarnings("deprecation")
public class ElasticsearchFunction extends AbstractFunction {

    private static final Counter SINK_KAFKA_COUNTER =
            Counter.build()
                    .name("sink_kafka_counter")
                    .help("sink kafka counter")
                    .labelNames("host", "id")
                    .register();

    protected transient RestHighLevelClient client;
    protected transient Counter.Child sinkCounterChild;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        client = new RestHighLevelClient(createRestClientBuilder(context));
        sinkCounterChild = labelHostId(SINK_KAFKA_COUNTER);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        int totalCount = 0;
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            foreachRecord(records, (key, value, allHeaders) -> {}, defaultSinkExtraHeaders());
            totalCount += sendKafka(iterator.next());
        }
        sinkCounterChild.inc(totalCount);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(client);
    }
}
