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
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;

import java.util.Iterator;

/** AssertCountFunction. */
public class AssertCountFunction extends AbstractFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AssertCountFunction.class);

    private long count;
    private long currentCount = 0;

    public static final ConfigOption<Long> OPTION_ASSERT_COUNT =
            ConfigOptions.key("count").longType().defaultValue(-1L);

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        count = context.get(OPTION_ASSERT_COUNT);
    }

    @Override
    public void process(Offset offset, Iterator<Records> iterator) {
        while (iterator.hasNext()) {
            for (Record ignore : iterator.next()) {
                currentCount++;
            }
        }
    }

    @Override
    public void close() {
        LOG.info("assert count, expect {}, real {}", count, currentCount);
        Assert.assertEquals(count, currentCount);
    }
}
