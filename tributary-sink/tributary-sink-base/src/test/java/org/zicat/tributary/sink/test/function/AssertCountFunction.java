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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;

/** AssertCountFunction. */
public class AssertCountFunction extends AbstractFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AssertCountFunction.class);

    private long count;
    private long offset = 0;
    public static final String KEY_ASSERT_COUNT = "count";

    @Override
    public void open(Context context) {
        super.open(context);
        count = context.getCustomProperty(KEY_ASSERT_COUNT, -1);
    }

    @Override
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {
        while (iterator.hasNext()) {
            iterator.next();
            offset++;
        }
    }

    @Override
    public void close() {
        LOG.info("assert count, expect {}, real {}", count, offset);
        Assert.assertEquals(count, offset);
    }
}
