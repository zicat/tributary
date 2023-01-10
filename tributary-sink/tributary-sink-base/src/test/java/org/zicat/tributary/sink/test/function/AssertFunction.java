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
import org.zicat.tributary.queue.RecordsOffset;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/** AssertFunction. */
public class AssertFunction extends AbstractFunction {

    private List<?> assertData;
    public static final String KEY_ASSERT_DATA = "assert.data";

    @Override
    public void open(Context context) {
        super.open(context);
        // important: assert data list must thread safe.
        assertData = (List<?>) context.getCustomProperty(KEY_ASSERT_DATA);
    }

    @Override
    public void process(RecordsOffset recordsOffset, Iterator<byte[]> iterator) {

        while (iterator.hasNext()) {
            Assert.assertFalse(assertData.isEmpty());
            byte[] data = iterator.next();
            String value = new String(data, StandardCharsets.UTF_8);
            Assert.assertTrue(assertData.remove(value));
            flush(recordsOffset, null);
        }
    }

    @Override
    public void close() {}
}
