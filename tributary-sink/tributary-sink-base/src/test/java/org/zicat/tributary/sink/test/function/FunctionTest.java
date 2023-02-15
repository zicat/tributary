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
import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.sink.function.*;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;

/** FunctionTest. */
public class FunctionTest {

    @Test
    public void testAbstractFunction() {

        final AbstractFunction function =
                new AbstractFunction() {
                    @Override
                    public void close() {}

                    @Override
                    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {}
                };
        final RecordsOffset recordsOffset = new RecordsOffset(1, 0, "g1");
        final ContextBuilder builder =
                ContextBuilder.newBuilder().startRecordsOffset(recordsOffset).partitionId(1);
        final Context context = builder.build();
        function.open(context);
        Assert.assertEquals(function.committableOffset(), recordsOffset);
        Assert.assertEquals(context.groupId(), function.context().groupId());
        Assert.assertEquals(context.partitionId(), function.context().partitionId());
        Assert.assertNull(context.topic(), function.context().topic());
        Assert.assertEquals(context, function.context());

        RecordsOffset newRecordsOffset = recordsOffset.skipNextSegmentHead();
        function.flush(newRecordsOffset, null);
        Assert.assertEquals(function.committableOffset(), newRecordsOffset);
    }

    @Test
    public void testDummyFunction() throws Exception {

        final Function function = new DummyFunction();
        final RecordsOffset recordsOffset = new RecordsOffset(1, 0, "g1");
        final ContextBuilder builder =
                ContextBuilder.newBuilder().startRecordsOffset(recordsOffset).partitionId(1);
        final Context context = builder.build();
        function.open(context);

        final RecordsOffset newRecordsOffset = recordsOffset.skip2TargetHead(2);
        function.process(
                recordsOffset.skip2TargetHead(2),
                Collections.singleton("data".getBytes(StandardCharsets.UTF_8)).iterator());
        Assert.assertEquals(function.committableOffset(), newRecordsOffset);
    }
}
