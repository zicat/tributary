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
import org.zicat.tributary.common.metric.MetricCollector;
import org.zicat.tributary.common.metric.MetricKey;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.config.Context;
import org.zicat.tributary.sink.handler.DirectPartitionHandler;
import org.zicat.tributary.sink.handler.MultiThreadPartitionHandler;
import org.zicat.tributary.sink.handler.PartitionHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Function.
 *
 * <p>{@link PartitionHandler} create at least one Function and each Function only bind one thread.
 */
public interface Function extends Closeable, CheckpointedFunction, MetricCollector {

    /**
     * open the function.
     *
     * @param context context
     * @throws Exception Exception
     */
    void open(Context context) throws Exception;

    /**
     * process the data in one partition offset.
     *
     * <p>note: In some SinkHandler{@link MultiThreadPartitionHandler}, One Function Instance only
     * consumer parts data of the partition
     *
     * <p>In some SinkHandler{@link DirectPartitionHandler}, One Function Instance consumer all data
     * of the partition
     *
     * @param offset offset
     * @param iterator iterator
     */
    void process(Offset offset, Iterator<Records> iterator) throws Exception;

    /**
     * return the committable partition offset.
     *
     * @return map, key is partition id, value is the committable partition offset
     */
    Offset committableOffset();

    /** default snapshot, subclass can override this function. */
    @Override
    default void snapshot() throws Exception {}

    /**
     * gauge family metric.
     *
     * @return map
     */
    @Override
    default Map<MetricKey, Double> gaugeFamily() {
        return Collections.emptyMap();
    }

    /**
     * count family metric.
     *
     * @return map
     */
    @Override
    default Map<MetricKey, Double> counterFamily() {
        return Collections.emptyMap();
    }

    /**
     * close function.
     *
     * <p>if invoke {@link #open(Context)} fail, this method still invoke.
     *
     * @throws IOException IOException
     */
    void close() throws IOException;

    /**
     * function id.
     *
     * @return string id
     */
    String functionId();

    /**
     * group id.
     *
     * @return group id
     */
    String groupId();

    /**
     * topic.
     *
     * @return topic
     */
    String topic();

    /**
     * partition id.
     *
     * @return partition id
     */
    int partitionId();
}
