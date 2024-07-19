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
import org.zicat.tributary.sink.function.*;

import java.util.Collections;
import java.util.Iterator;

import static org.zicat.tributary.common.records.RecordsUtils.createStringRecords;

/** FunctionTest. */
public class FunctionTest {

    @Test
    public void testAbstractFunction() throws Exception {

        try (final AbstractFunction function =
                new AbstractFunction() {
                    @Override
                    public void close() {}

                    @Override
                    public void process(Offset offset, Iterator<Records> iterator) {}
                }) {
            final Offset offset = new Offset(1, 0);
            final ContextBuilder builder =
                    ContextBuilder.newBuilder()
                            .id("1")
                            .topic("t1")
                            .groupId("g1")
                            .startOffset(offset)
                            .partitionId(1);
            final Context context = builder.build();
            function.open(context);
            Assert.assertEquals(function.committableOffset(), offset);
            Assert.assertEquals(context.groupId(), function.context().groupId());
            Assert.assertEquals(context.partitionId(), function.context().partitionId());
            Assert.assertEquals(context.topic(), function.context().topic());
            Assert.assertEquals(context, function.context());

            final Offset newOffset = offset.skipNextSegmentHead();
            function.commit(newOffset);
            Assert.assertEquals(function.committableOffset(), newOffset);
        }
    }

    @Test
    public void testDummyFunction() throws Exception {
        try (final Function function = new DummyFunction()) {
            final Offset offset = new Offset(1, 0);
            final ContextBuilder builder =
                    ContextBuilder.newBuilder()
                            .id("1")
                            .groupId("g1")
                            .topic("t1")
                            .startOffset(offset)
                            .partitionId(1);
            final Context context = builder.build();
            function.open(context);

            final Offset newGroupOffset = offset.skip2TargetHead(2);
            function.process(
                    offset.skip2TargetHead(2),
                    Collections.singletonList(createStringRecords("t1", "data")).iterator());
            Assert.assertEquals(function.committableOffset(), newGroupOffset);
        }
    }
}
