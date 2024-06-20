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
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.records.RecordsIterator;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.Trigger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * DirectPartitionHandler.
 *
 * <p>One Thread mode, One ${@link DirectPartitionHandler} instance bind with one {@link Function}
 * instance .
 */
public class DirectPartitionHandler extends AbstractPartitionHandler {

    private AbstractFunction function;
    private Trigger trigger;

    public DirectPartitionHandler(
            String groupId, Channel channel, int partitionId, SinkGroupConfig sinkGroupConfig) {
        super(groupId, channel, partitionId, sinkGroupConfig);
    }

    @Override
    public void open() {
        this.function = createFunction(null);
        this.trigger = function instanceof Trigger ? (Trigger) function : null;
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) throws Exception {
        function.process(groupOffset, RecordsIterator.wrap(iterator));
    }

    @Override
    public GroupOffset committableOffset() {
        return function.committableOffset();
    }

    @Override
    public void closeCallback() throws IOException {
        function.close();
    }

    @Override
    public List<AbstractFunction> getFunctions() {
        return Collections.singletonList(function);
    }

    @Override
    public long idleTimeMillis() {
        return trigger == null ? -1 : trigger.idleTimeMillis();
    }

    public Function getFunction() {
        return function;
    }

    @Override
    public void idleTrigger() throws Throwable {
        if (trigger != null) {
            trigger.idleTrigger();
        }
    }
}
