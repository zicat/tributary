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

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import static org.zicat.tributary.sink.Config.OPTION_CLOCK;

/** AbstractFunctionTest. */
public class AbstractFunctionTest {

    final int fullMill = 10;

    @Test
    public void testFlush() {

        final GroupOffset startGroupOffset = new GroupOffset(0, 0, "g1");
        final MockClock clock = new MockClock();
        clock.setCurrentTimeMillis(0);
        final MockFunction function = createFunction(clock);
        final AtomicInteger callback = new AtomicInteger();
        function.flush(
                startGroupOffset.skipNextSegmentHead(),
                () -> {
                    callback.incrementAndGet();
                    return true;
                });
        Assert.assertEquals(1, callback.get());
        clock.setCurrentTimeMillis(fullMill);
        function.flush(
                startGroupOffset.skipNextSegmentHead(),
                () -> {
                    callback.incrementAndGet();
                    return true;
                });
        Assert.assertEquals(2, callback.get());
    }

    /**
     * create function by clock.
     *
     * @param clock clock
     * @return MockFunction
     */
    private MockFunction createFunction(MockClock clock) {
        final MockFunction function = new MockFunction();
        final GroupOffset startGroupOffset = new GroupOffset(0, 0, "g1");
        final ContextBuilder builder =
                ContextBuilder.newBuilder().startGroupOffset(startGroupOffset).partitionId(1);
        builder.addCustomProperty(OPTION_CLOCK.key(), clock);
        final Context context = builder.build();
        clock.setCurrentTimeMillis(0);
        function.open(context);
        return function;
    }

    /** MockFunction. */
    private static class MockFunction extends AbstractFunction {

        @Override
        public void process(GroupOffset groupOffset, Iterator<byte[]> iterator) {}

        @Override
        public void close() {}
    }
}
