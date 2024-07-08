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

package org.zicat.tributary.sink.kafka.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.sink.kafka.TributaryKafkaCallback;

/** TributaryKafkaCallbackTest. */
public class TributaryKafkaCallbackTest {

    @Test
    public void test() throws Exception {
        final TributaryKafkaCallback callback = new TributaryKafkaCallback();
        callback.onCompletion(null, null);
        callback.checkState();

        final Exception e = new IllegalArgumentException();
        callback.onCompletion(null, e);
        try {
            callback.checkState();
            Assert.fail();
        } catch (IllegalArgumentException error) {
            Assert.assertEquals(error, e);
        }
        callback.checkState();
    }
}
