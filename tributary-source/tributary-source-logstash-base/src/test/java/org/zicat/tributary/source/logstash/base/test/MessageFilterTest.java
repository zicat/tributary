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

package org.zicat.tributary.source.logstash.base.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.IOUtils;
import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.base.MessageFilter;
import org.zicat.tributary.source.logstash.base.utils.CompileUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** MessageFilterTest. */
public class MessageFilterTest {

    @Test
    public void test() throws IOException, InstantiationException, IllegalAccessException {
        final byte[] code =
                IOUtils.readFully(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("DefaultMessageFilter.txt")));
        final byte[] code2 =
                IOUtils.readFully(
                        Objects.requireNonNull(
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("DefaultMessageFilter2.txt")));
        final Class<MessageFilter<Object>> filterClass =
                CompileUtils.doCompile(
                        "DefaultMessageFilter", new String(code, StandardCharsets.UTF_8));
        final MessageFilter<Object> filter = filterClass.newInstance();
        final Map<String, Object> data = new HashMap<>();
        data.put("aa", "111");
        data.put("bb", "222");
        Assert.assertTrue(filter.filter(new Message<>(1, data)));
        Assert.assertNull(data.get("aa"));

        final Class<MessageFilter<Object>> filterClass2 =
                CompileUtils.doCompile(
                        "DefaultMessageFilter", new String(code2, StandardCharsets.UTF_8));
        final MessageFilter<Object> filter2 = filterClass2.newInstance();
        final Map<String, Object> data2 = new HashMap<>();
        data2.put("aa", "111");
        data2.put("bb", "222");
        Assert.assertTrue(filter2.filter(new Message<>(1, data2)));
        Assert.assertNull(data2.get("bb"));

        filter.filter(new Message<>(1, data2));
    }
}
