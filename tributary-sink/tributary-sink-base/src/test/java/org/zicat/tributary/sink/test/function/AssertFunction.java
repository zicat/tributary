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
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.config.ConfigOption;
import org.zicat.tributary.common.config.ConfigOptions;
import org.zicat.tributary.common.records.Record0;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.config.Context;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/** AssertFunction. */
public class AssertFunction extends AbstractFunction {

    public static final ConfigOption<List<?>> OPTION_ASSERT_DATA =
            ConfigOptions.key("assert.data")
                    .<List<?>>objectType()
                    .defaultValue(null);

    private List<?> assertData;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        // important: assert data list must thread safe.
        assertData = context.get(OPTION_ASSERT_DATA);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) {
        while (iterator.hasNext()) {
            for (Record0 record : iterator.next()) {
                Assert.assertFalse(assertData.isEmpty());
                String value = new String(record.value(), StandardCharsets.UTF_8);
                Assert.assertTrue(assertData.remove(value));
            }
        }
        commit(offset);
    }

    @Override
    public void close() {}
}
