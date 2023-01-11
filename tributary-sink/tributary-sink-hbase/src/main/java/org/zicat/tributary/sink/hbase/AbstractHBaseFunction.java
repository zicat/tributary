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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Mutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.utils.Exceptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/** AbstractHBaseFunction. */
public abstract class AbstractHBaseFunction extends AbstractFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseFunction.class);
    private final Map<HTableEntity, HBaseWriter> hbaseWriterMap = new HashMap<>();

    @Override
    public void open(Context context) {
        super.open(context);
    }

    /**
     * send data to hbase.
     *
     * @param hTableEntity hTableEntity
     * @param mutation mutation
     * @throws IOException IOException
     */
    protected boolean sendDataToHbase(HTableEntity hTableEntity, Mutation mutation)
            throws IOException {
        if (Objects.isNull(mutation)) {
            return false;
        }
        return createHBaseWriterIfAbsent(hTableEntity).appendData(mutation);
    }

    @Override
    public void close() throws IOException {
        IOException lastException = null;
        for (Map.Entry<HTableEntity, HBaseWriter> entry : hbaseWriterMap.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                if (lastException != null) {
                    LOG.warn("close error", lastException);
                }
                lastException = Exceptions.castAsIOException(e);
            }
        }
        hbaseWriterMap.clear();
        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * for hbase flush offsets 2 minutes earlier.
     *
     * @param fileOffset fileOffset
     */
    public boolean flush(RecordsOffset fileOffset) {
        final AtomicBoolean flushed = new AtomicBoolean(false);
        flush(
                fileOffset,
                () -> {
                    for (HBaseWriter hbaseWriterAbstract : hbaseWriterMap.values()) {
                        try {
                            hbaseWriterAbstract.flush();
                        } catch (IOException ioException) {
                            throw new RuntimeException(ioException);
                        }
                    }
                    flushed.set(true);
                    return true;
                });
        return flushed.get();
    }

    /**
     * create hbase writer.
     *
     * @param hTableEntity hTableEntity
     * @return HbaseWriter
     */
    protected HBaseWriter createHBaseWriterIfAbsent(HTableEntity hTableEntity) {
        return hbaseWriterMap.computeIfAbsent(
                hTableEntity,
                key -> {
                    try {
                        return createHBaseWriter(hTableEntity);
                    } catch (Exception e) {
                        LOG.error(hTableEntity + " data discard", e);
                        return new DiscardHBaseWriter();
                    }
                });
    }

    /**
     * create hbase writer.
     *
     * @param hTableEntity hTableEntity
     * @return HBaseWriter
     */
    protected HBaseWriter createHBaseWriter(HTableEntity hTableEntity) {
        return new AbstractHBaseWriter(hTableEntity) {

            @Override
            public Configuration createConfiguration(HTableEntity entity) {
                return AbstractHBaseFunction.this.createHBaseConf(entity);
            }

            @Override
            public BufferedMutatorParams createBufferedMutatorParams(HTableEntity hTableEntity1) {
                return AbstractHBaseFunction.this.createBufferedMutatorParams(hTableEntity1);
            }
        }.open();
    }

    /**
     * get hbase writer by hTableEntity.
     *
     * @param hTableEntity hTableEntity
     * @return HBaseWriter
     */
    public final HBaseWriter getHBaseWriter(HTableEntity hTableEntity) {
        return hbaseWriterMap.get(hTableEntity);
    }

    /**
     * create hbase configuration.
     *
     * @param hTableEntity hTableEntity
     * @return Configuration
     */
    public abstract Configuration createHBaseConf(HTableEntity hTableEntity);

    /**
     * create buffer mutator params.
     *
     * @param hTableEntity hTableEntity
     * @return BufferedMutatorParams
     */
    public abstract BufferedMutatorParams createBufferedMutatorParams(HTableEntity hTableEntity);

    /**
     * get hBase connection count, for testing.
     *
     * @return size
     */
    public final int hBaseConnectionCount() {
        return hbaseWriterMap.size();
    }
}
