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

package org.zicat.tributary.source.base.test.netty;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.config.ReadableConfigConfigBuilder;
import static org.zicat.tributary.source.base.AbstractSource.OPTION_CHANNEL_APPEND_RESULT_TYPE;
import org.zicat.tributary.source.base.AppendResultType;

/** AppendResultTypeTest. */
public class AppendResultTypeTest {

    @Test
    public void test() {
        ReadableConfigConfigBuilder builder = new ReadableConfigConfigBuilder();
        builder.addConfig(OPTION_CHANNEL_APPEND_RESULT_TYPE.key(), "block");
        Assert.assertEquals(
                AppendResultType.BLOCK, builder.build().get(OPTION_CHANNEL_APPEND_RESULT_TYPE));

        builder.addConfig(OPTION_CHANNEL_APPEND_RESULT_TYPE.key(), "storage");
        Assert.assertEquals(
                AppendResultType.STORAGE, builder.build().get(OPTION_CHANNEL_APPEND_RESULT_TYPE));
    }
}
