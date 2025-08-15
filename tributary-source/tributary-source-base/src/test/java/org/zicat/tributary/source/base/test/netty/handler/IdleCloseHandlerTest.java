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

package org.zicat.tributary.source.base.test.netty.handler;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateHandler;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.source.base.netty.handler.IdleCloseHandler;

/** IdleCloseHandlerTest. */
public class IdleCloseHandlerTest {

    @Test
    public void test() throws InterruptedException {
        final int idleSecond = 1;
        final EmbeddedChannel channel =
                new EmbeddedChannel(
                        new IdleStateHandler(idleSecond, idleSecond, idleSecond),
                        new IdleCloseHandler());
        Assert.assertTrue(channel.isActive());
        Thread.sleep(1200);
        // trigger idle
        channel.writeOneOutbound(ByteBufAllocator.DEFAULT.buffer(10).writeInt(10));
        Assert.assertFalse(channel.isActive());
    }
}
