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

package org.zicat.tributary.sink.elasticsearch.test.listener;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.channel.Offset;
import static org.zicat.tributary.common.SpiFactory.findFactory;
import org.zicat.tributary.sink.elasticsearch.listener.AbstractActionListener;
import org.zicat.tributary.sink.elasticsearch.listener.BulkResponseActionListenerFactory;
import static org.zicat.tributary.sink.elasticsearch.listener.MuteInsertErrorBulkResponseActionListener.ERROR_COUNTER;
import static org.zicat.tributary.sink.elasticsearch.listener.MuteInsertErrorBulkResponseActionListener.LABEL_INDEX;
import org.zicat.tributary.sink.elasticsearch.listener.MuteInsertErrorBulkResponseActionListenerFactory;
import org.zicat.tributary.sink.function.ContextBuilder;

import java.util.concurrent.TimeUnit;

/** MuteInsertErrorBulkResponseActionListenerFactoryTest. */
public class MuteInsertErrorBulkResponseActionListenerFactoryTest {

    @Test
    public void test() throws InterruptedException {

        ContextBuilder builder =
                ContextBuilder.newBuilder()
                        .id("1")
                        .groupId("g1")
                        .topic("t1")
                        .partitionId(1)
                        .startOffset(Offset.ZERO);

        BulkResponseActionListenerFactory factory =
                findFactory(
                        MuteInsertErrorBulkResponseActionListenerFactory.IDENTITY,
                        BulkResponseActionListenerFactory.class);
        final AbstractActionListener listener = factory.create(builder.build(), null, Offset.ZERO);
        Assert.assertTrue(listener.isRunning());
        try {
            Exception e = listener.exception();
            Assert.fail(e.getMessage());
        } catch (IllegalStateException e) {
            Assert.assertEquals("ActionListener is not done.", e.getMessage());
        }

        final RuntimeException runtimeException = new RuntimeException();
        new Thread(() -> listener.onFailure(runtimeException)).start();

        listener.awaitDone(5000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(listener.isRunning());
        Assert.assertEquals(Offset.ZERO, listener.offset());
        Assert.assertEquals(runtimeException, listener.exception());

        final AbstractActionListener listener2 = factory.create(builder.build(), null, Offset.ZERO);
        new Thread(() -> listener2.onResponse(new BulkResponse(new BulkItemResponse[] {}, 1)))
                .start();
        listener2.awaitDone(5000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(listener2.isRunning());
        Assert.assertEquals(Offset.ZERO, listener2.offset());
        Assert.assertNull(listener2.exception());

        BulkItemResponse itemResponse =
                BulkItemResponse.failure(
                        1, OpType.INDEX, new Failure("index_1", "type", "id", runtimeException));

        final AbstractActionListener listener3 = factory.create(builder.build(), null, Offset.ZERO);
        new Thread(
                        () ->
                                listener3.onResponse(
                                        new BulkResponse(new BulkItemResponse[] {itemResponse}, 1)))
                .start();
        listener3.awaitDone(5000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(listener3.isRunning());
        Assert.assertEquals(Offset.ZERO, listener3.offset());
        Assert.assertNull(listener3.exception());
        Assert.assertEquals(
                1d,
                listener3.counterFamily().get(ERROR_COUNTER.addLabel(LABEL_INDEX, "index_1")),
                0.001d);
    }
}
