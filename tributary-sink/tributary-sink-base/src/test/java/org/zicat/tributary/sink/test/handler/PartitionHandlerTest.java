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

package org.zicat.tributary.sink.test.handler;

import org.junit.Test;
import org.zicat.tributary.sink.handler.factory.DirectPartitionHandlerFactory;
import org.zicat.tributary.sink.handler.factory.MultiThreadPartitionHandlerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** SinkHandlerTest. */
public class PartitionHandlerTest {

    @Test
    public void testSimpleSinkHandler() throws IOException {
        SinkHandlerTestBase.test(createTestData(100), DirectPartitionHandlerFactory.IDENTITY);
    }

    @Test
    public void testDisruptorSinkHandler() throws IOException {
        SinkHandlerTestBase.test(createTestData(300), MultiThreadPartitionHandlerFactory.IDENTITY);
    }

    /**
     * create test data.
     *
     * @param count size
     * @return list string
     */
    private static List<String> createTestData(int count) {
        final List<String> result = new ArrayList<>(count);
        final Random random = new Random();
        for (int i = 0; i < count; i++) {
            result.add(random.nextInt(10) + "aaaabbbb" + count);
        }
        return Collections.synchronizedList(result);
    }
}
