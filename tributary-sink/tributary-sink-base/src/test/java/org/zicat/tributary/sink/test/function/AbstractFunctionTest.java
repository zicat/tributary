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
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.ContextBuilder;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/** AbstractFunctionTest. */
public class AbstractFunctionTest {

    @Test
    public void testFlush() throws Exception {

        final Offset startOffset = new Offset(0, 0);
        final AtomicInteger callback = new AtomicInteger();
        try (final MockFunction function = createFunction(callback)) {
            function.commit(startOffset.skipNextSegmentHead());
            Assert.assertEquals(1, callback.get());
            function.commit(startOffset.skipNextSegmentHead());
            Assert.assertEquals(2, callback.get());
        }
    }

    /**
     * create function by clock.
     *
     * @return MockFunction
     */
    private MockFunction createFunction(AtomicInteger callback) throws Exception {
        final MockFunction function = new MockFunction(callback);
        final ContextBuilder builder =
                ContextBuilder.newBuilder().id("1").groupId("g1").topic("t1").partitionId(1);
        final Context context = builder.build();
        function.open(context);
        return function;
    }

    /** MockFunction. */
    private static class MockFunction extends AbstractFunction {

        public final AtomicInteger callback;

        public MockFunction(AtomicInteger callback) {
            this.callback = callback;
        }

        @Override
        public void process(Offset offset, Iterator<Records> iterator) {}

        @Override
        public void commit(Offset newCommittableOffset) {
            callback.incrementAndGet();
            super.commit(newCommittableOffset);
        }

        @Override
        public void close() {}
    }
}
