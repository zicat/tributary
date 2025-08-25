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

package org.zicat.tributary.sink.hbase;

import static org.zicat.tributary.common.IOUtils.getClasspathResource;
import static org.zicat.tributary.common.records.RecordsUtils.defaultSinkExtraHeaders;
import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;
import static org.zicat.tributary.sink.hbase.HBaseFunctionFactory.*;

import io.prometheus.client.Counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.common.TributaryRuntimeException;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** HBaseFunction. */
public class HBaseFunction extends AbstractFunction implements BufferedMutator.ExceptionListener {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseFunction.class);

    private static final Counter SINK_HBASE_COUNTER =
            Counter.build()
                    .name("sink_hbase_counter")
                    .help("sink hbase counter")
                    .labelNames("host", "id")
                    .register();
    private static final Counter SINK_HBASE_DISCARD_COUNTER =
            Counter.build()
                    .name("sink_hbase_discard_counter")
                    .help("sink hbase discard counter")
                    .labelNames("host", "id")
                    .register();
    protected transient Counter.Child sinkCounter;
    protected transient Counter.Child sinkDiscardCounter;
    protected transient TableName tableName;
    protected transient byte[] family;
    protected transient byte[] valueColumn;
    protected transient byte[] topicColumn;
    protected transient String headColumnPrefix;
    protected transient Connection connection;
    protected transient BufferedMutator mutator;
    protected transient AtomicReference<Exception> failureThrowable;
    protected transient Offset lastOffset;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        sinkCounter = labelHostId(SINK_HBASE_COUNTER);
        sinkDiscardCounter = labelHostId(SINK_HBASE_DISCARD_COUNTER);
        tableName = TableName.valueOf(context.get(OPTION_HBASE_TABLE_NAME));
        family = Bytes.toBytes(context.get(OPTION_HBASE_FAMILY));
        valueColumn = Bytes.toBytes(context.get(OPTION_HBASE_COLUMN_VALUE_NAME));
        topicColumn = Bytes.toBytes(context.get(OPTION_HBASE_COLUMN_TOPIC_NAME));
        headColumnPrefix = context.get(OPTION_HBASE_COLUMN_HEAD_PREFIX);
        connection = createConnection();
        mutator = createBufferedMutator();
        failureThrowable = new AtomicReference<>();
        LOG.info("Create hbase writer succeed {} ", tableName.getNameAsString());
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final AtomicInteger discardCounter = new AtomicInteger();
        while (iterator.hasNext()) {
            final Records records = iterator.next();
            foreachRecord(
                    records,
                    (key, value, allHeaders) -> {
                        if (key == null) {
                            discardCounter.incrementAndGet();
                            return;
                        }
                        mutate(records.topic(), key, value, allHeaders);
                        counter.incrementAndGet();
                    },
                    defaultSinkExtraHeaders());
        }
        lastOffset = offset;
        sinkCounter.inc(counter.get());
        sinkDiscardCounter.inc(discardCounter.get());
    }

    @Override
    public void snapshot() throws Exception {
        mutator.flush();
        checkErrorAndRethrow();
        commit(lastOffset);
    }

    /**
     * create buffered mutator.
     *
     * @return BufferedMutator
     * @throws IOException IOException
     */
    protected BufferedMutator createBufferedMutator() throws IOException {
        if (tableName == null) {
            throw new NullPointerException("table is null");
        }
        if (connection == null) {
            throw new NullPointerException("connection is null");
        }
        final BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        params.implementationClassName(BufferedMutatorImpl2.class.getName()).listener(this);
        return connection.getBufferedMutator(params);
    }

    /**
     * create connection.
     *
     * @return connection
     */
    protected Connection createConnection() throws IOException {
        final Configuration conf = createConfiguration();
        final Connection connection = ConnectionFactory.createConnection(conf);
        LOG.info(
                "Create hbase connection on {} succeed zk:{} ",
                context.id(),
                conf.get(HConstants.ZOOKEEPER_QUORUM));
        return connection;
    }

    /**
     * create configuration.
     *
     * @return configuration
     */
    protected Configuration createConfiguration() {
        final Configuration conf = HBaseConfiguration.create();
        final String hbaseSiteXmlPath = context.get(OPTION_HBASE_SITE_XML_PATH);
        if (hbaseSiteXmlPath != null) {
            final InputStream is = getClasspathResource(hbaseSiteXmlPath);
            if (is != null) {
                conf.addResource(is);
            } else {
                conf.addResource(new Path(hbaseSiteXmlPath));
            }
        }
        final String zkConfig = conf.get(HConstants.ZOOKEEPER_QUORUM);
        if (zkConfig == null || zkConfig.trim().isEmpty()) {
            throw new TributaryRuntimeException(HConstants.ZOOKEEPER_QUORUM + " not found");
        }
        return conf;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(mutator, connection);
    }

    @Override
    public void onException(
            RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) {
        failureThrowable.compareAndSet(null, exception);
    }

    /**
     * mutate data.
     *
     * @param key key
     * @param value value
     * @param headers headers
     * @throws Exception Exception
     */
    protected void mutate(String topic, byte[] key, byte[] value, Map<String, byte[]> headers)
            throws Exception {
        /*
           disable wal for performance.
           tributary is a reliable system that can tolerate data lost with wal by channel
        */
        final Put put =
                new Put(key)
                        .addColumn(family, topicColumn, Bytes.toBytes(topic))
                        .addColumn(family, valueColumn, value)
                        .setDurability(Durability.SKIP_WAL);
        for (Entry<String, byte[]> entry : headers.entrySet()) {
            final byte[] headColumn = Bytes.toBytes(headColumnPrefix + entry.getKey());
            put.addColumn(family, headColumn, entry.getValue());
        }
        mutator.mutate(put);
    }

    /** check error and rethrow. */
    private void checkErrorAndRethrow() throws Exception {
        final Exception cause = failureThrowable.get();
        if (cause != null && failureThrowable.compareAndSet(cause, null)) {
            throw cause;
        }
    }
}
