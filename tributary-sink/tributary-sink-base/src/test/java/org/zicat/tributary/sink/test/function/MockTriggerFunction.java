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

package org.zicat.tributary.sink.test.function;

import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Trigger;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/** MockTriggerFunction. */
public class MockTriggerFunction extends AbstractFunction implements Trigger {

    public AtomicInteger idleTriggerCounter = new AtomicInteger();
    public static final long IDLE_TIME_MILLIS = 10;

    @Override
    public void process(GroupOffset groupOffset, Iterator<Records> iterator) {}

    @Override
    public void close() {}

    @Override
    public long idleTimeMillis() {
        return IDLE_TIME_MILLIS;
    }

    @Override
    public void idleTrigger() {
        idleTriggerCounter.incrementAndGet();
    }
}
