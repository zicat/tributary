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

package org.zicat.tributary.sink.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.sink.SinkGroupConfig;
import org.zicat.tributary.sink.SinkGroupConfigBuilder;
import org.zicat.tributary.sink.function.PrintFunctionFactory;
import org.zicat.tributary.sink.handler.DefaultPartitionHandlerFactory;
import org.zicat.tributary.sink.handler.DirectPartitionHandlerFactory;

/** SinkGroupConfigBuilderTest. */
public class SinkGroupConfigBuilderTest {

    @Test
    public void test() {
        final SinkGroupConfig config =
                SinkGroupConfigBuilder.newBuilder()
                        .functionIdentity(PrintFunctionFactory.IDENTITY)
                        .build();
        Assert.assertEquals(DefaultPartitionHandlerFactory.IDENTITY, config.handlerIdentity());
        Assert.assertEquals(PrintFunctionFactory.IDENTITY, config.functionIdentity());

        final SinkGroupConfig config2 =
                SinkGroupConfigBuilder.newBuilder()
                        .handlerIdentity(DirectPartitionHandlerFactory.IDENTITY)
                        .functionIdentity(PrintFunctionFactory.IDENTITY)
                        .build();
        Assert.assertEquals(DirectPartitionHandlerFactory.IDENTITY, config2.handlerIdentity());

        try {
            SinkGroupConfigBuilder.newBuilder().build();
            Assert.fail();
        } catch (IllegalStateException ignore) {
        }
    }
}
