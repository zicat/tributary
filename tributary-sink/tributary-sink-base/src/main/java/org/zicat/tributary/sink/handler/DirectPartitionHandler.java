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

import lombok.extern.slf4j.Slf4j;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.Trigger;

import java.io.IOException;
import java.util.Iterator;

/**
 * SimplePartitionHandler.
 *
 * <p>Single Thread mode, One ${@link DirectPartitionHandler} instance bind with one {@link
 * Function} instance .
 */
@Slf4j
public class DirectPartitionHandler extends AbstractPartitionHandler {

    private Function function;
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
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) throws Exception {
        function.process(recordsOffset, iterator);
    }

    @Override
    public RecordsOffset committableOffset() {
        return function.committableOffset();
    }

    @Override
    public void closeCallback() throws IOException {
        function.close();
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
