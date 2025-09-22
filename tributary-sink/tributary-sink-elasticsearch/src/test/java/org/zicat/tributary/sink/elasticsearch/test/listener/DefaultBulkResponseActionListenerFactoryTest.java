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
import org.zicat.tributary.sink.elasticsearch.listener.DefaultBulkResponseActionListenerFactory;

import java.util.concurrent.TimeUnit;

/** DefaultActionListenerFactoryTest. */
public class DefaultBulkResponseActionListenerFactoryTest {

    @Test
    public void test() throws InterruptedException {
        BulkResponseActionListenerFactory factory =
                findFactory(
                        DefaultBulkResponseActionListenerFactory.IDENTITY,
                        BulkResponseActionListenerFactory.class);
        final AbstractActionListener listener = factory.create(null, null, Offset.ZERO);
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

        final AbstractActionListener listener2 = factory.create(null, null, Offset.ZERO);
        new Thread(() -> listener2.onResponse(new BulkResponse(new BulkItemResponse[] {}, 1)))
                .start();
        listener2.awaitDone(5000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(listener2.isRunning());
        Assert.assertEquals(Offset.ZERO, listener2.offset());
        Assert.assertNull(listener2.exception());

        BulkItemResponse itemResponse =
                BulkItemResponse.failure(
                        1, OpType.INDEX, new Failure("index", "type", "id", runtimeException));

        final AbstractActionListener listener3 = factory.create(null, null, Offset.ZERO);
        new Thread(
                        () ->
                                listener3.onResponse(
                                        new BulkResponse(new BulkItemResponse[] {itemResponse}, 1)))
                .start();
        listener3.awaitDone(5000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(listener3.isRunning());
        Assert.assertEquals(Offset.ZERO, listener3.offset());
        Assert.assertNotNull(listener3.exception());
    }
}
