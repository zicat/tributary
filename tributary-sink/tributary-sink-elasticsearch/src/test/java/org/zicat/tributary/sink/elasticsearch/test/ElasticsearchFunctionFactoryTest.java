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

package org.zicat.tributary.sink.elasticsearch.test;

import org.junit.Assert;
import org.junit.Test;
import org.zicat.tributary.common.SpiFactory;
import org.zicat.tributary.sink.elasticsearch.ElasticsearchFunction;
import org.zicat.tributary.sink.elasticsearch.ElasticsearchFunctionFactory;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.io.IOException;

/** ElasticsearchFunctionFactoryTest. */
public class ElasticsearchFunctionFactoryTest {

    @Test
    public void test() throws IOException {
        try (Function f =
                SpiFactory.findFactory(ElasticsearchFunctionFactory.IDENTITY, FunctionFactory.class)
                        .create()) {
            Assert.assertEquals(ElasticsearchFunction.class, f.getClass());
        }
    }
}
