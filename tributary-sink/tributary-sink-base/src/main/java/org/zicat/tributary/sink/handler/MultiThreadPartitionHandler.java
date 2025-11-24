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

import static org.zicat.tributary.common.util.Collections.deepCopy;

import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.util.IOUtils;
import org.zicat.tributary.common.util.Threads;
import org.zicat.tributary.common.records.RecordsIterator;
import org.zicat.tributary.sink.config.SinkGroupConfig;
import org.zicat.tributary.sink.function.Function;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Disruptor sink handler.
 *
 * <p>Multi Threads mode, One ${@link MultiThreadPartitionHandler} instance bind with at least one
 * {@link Function} instance
 *
 * <p>.
 */
public class MultiThreadPartitionHandler extends PartitionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MultiThreadPartitionHandler.class);

    private final Disruptor<Block> disruptor;
    private final DataHandler[] handlers;
    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final int workerNumber;

    public MultiThreadPartitionHandler(
            String groupId,
            Channel channel,
            int partitionId,
            int workerNumber,
            SinkGroupConfig config) {
        super(groupId, channel, partitionId, config);
        if (workerNumber <= 1) {
            throw new IllegalArgumentException(
                    "worker number must > 1, real value " + workerNumber);
        }
        this.workerNumber = workerNumber;
        this.handlers = new DataHandler[workerNumber];
        this.disruptor = createDisruptor(cap(workerNumber * 3));
    }

    @Override
    public void open() {
        for (int i = 0; i < workerNumber; i++) {
            handlers[i] = new DataHandler(createFunction(createFunctionId(i)), error);
            LOG.info("MultiThread Data handler initialed, GroupId:{}, Id:{}", groupId, i);
        }
        disruptor.handleEventsWithWorkerPool(handlers);
        disruptor.start();
    }

    /**
     * create function id.
     *
     * @param offset offset
     * @return string
     */
    public String createFunctionId(int offset) {
        return getSinHandlerId() + "_" + offset;
    }

    /**
     * return handlers.
     *
     * @return handler array
     */
    public DataHandler[] handlers() {
        return handlers;
    }

    /**
     * create disruptor, subclass can override this function.
     *
     * @return disruptor
     */
    protected Disruptor<Block> createDisruptor(int buffSize) {
        return new Disruptor<>(
                Block::new,
                buffSize,
                Threads.createThreadFactoryByName(threadName() + "-", true),
                ProducerType.SINGLE,
                new TimeoutBlockingWaitStrategy(30, TimeUnit.SECONDS));
    }

    @Override
    public void process(Offset offset, Iterator<byte[]> iterator) throws Exception {
        checkProcessError();
        disruptor.publishEvent(
                (block, sequence) -> {
                    /*
                     *  After process finished, source iterator will fill next data from channel.
                     *  Put iterator to memory queue may cause data lost or exception,
                     *  because Consumer Thread may deal with the iterator later than next process.
                     */
                    final Iterator<byte[]> copy = deepCopy(iterator);
                    block.setIterator(copy);
                    block.setOffset(offset);
                });
    }

    /**
     * check process error.
     *
     * @throws Exception Exception
     */
    private void checkProcessError() throws Exception {
        final Throwable e = error.get();
        if (e != null) {
            error.set(null);
            throw (e instanceof Exception ? (Exception) e : new Exception(e));
        }
    }

    @Override
    public void close() throws IOException {
        super.close(
                () -> {
                    try {
                        IOUtils.concurrentCloseQuietly(handlers);
                    } finally {
                        disruptor.shutdown();
                    }
                });
    }

    @Override
    public Offset committableOffset() {
        Offset min = null;
        for (int i = 1; i < handlers.length; i++) {
            final Offset offset = handlers[i].function.committableOffset();
            min = min == null ? offset : Offset.min(min, offset);
        }
        return min == null ? startOffset : min;
    }

    @Override
    public List<Function> getFunctions() {
        return Arrays.stream(handlers).map(v -> v.function).collect(Collectors.toList());
    }

    @Override
    public Map<MetricKey, Double> gaugeFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.gaugeFamily());
        for (Function function : getFunctions()) {
            for (Map.Entry<MetricKey, Double> entry : function.gaugeFamily().entrySet()) {
                base.merge(entry.getKey(), entry.getValue(), Double::sum);
            }
        }
        return base;
    }

    @Override
    public Map<MetricKey, Double> counterFamily() {
        final Map<MetricKey, Double> base = new HashMap<>(super.counterFamily());
        for (Function function : getFunctions()) {
            for (Map.Entry<MetricKey, Double> entry : function.counterFamily().entrySet()) {
                base.merge(entry.getKey(), entry.getValue(), Double::sum);
            }
        }
        return base;
    }

    @Override
    public void snapshot() throws Exception {
        for (DataHandler handler : handlers) {
            handler.snapshot();
        }
    }

    /** data handler. */
    public static class DataHandler implements WorkHandler<Block>, Closeable {

        private final Function function;
        private final AtomicReference<Throwable> error;

        public DataHandler(Function function, AtomicReference<Throwable> error) {
            this.function = function;
            this.error = error;
        }

        @Override
        public void onEvent(Block block) {
            if (Objects.nonNull(block) && block.getIterator() != null) {
                synchronized (this) {
                    try {
                        function.process(block.offset, RecordsIterator.wrap(block.getIterator()));
                    } catch (Throwable e) {
                        if (!error.compareAndSet(null, e)) {
                            LOG.error("process function error", e);
                        }
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            function.close();
        }

        public void snapshot() throws Exception {
            function.snapshot();
        }

        public String functionId() {
            return function.functionId();
        }
    }

    static int cap(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }

    /** block. */
    public static class Block {

        private Iterator<byte[]> iterator;
        private Offset offset;

        public Iterator<byte[]> getIterator() {
            return iterator;
        }

        public void setIterator(Iterator<byte[]> iterator) {
            this.iterator = iterator;
        }

        public Offset getOffset() {
            return offset;
        }

        public void setOffset(Offset offset) {
            this.offset = offset;
        }
    }
}
