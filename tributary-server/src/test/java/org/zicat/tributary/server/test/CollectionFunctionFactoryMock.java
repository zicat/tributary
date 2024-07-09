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

package org.zicat.tributary.server.test;

import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.records.DefaultRecord;
import org.zicat.tributary.common.records.Record;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.sink.function.AbstractFunction;
import org.zicat.tributary.sink.function.Context;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.util.Iterator;
import java.util.List;

import static org.zicat.tributary.common.records.RecordsUtils.foreachRecord;

/** CollectionFunctionFactoryMock. */
public class CollectionFunctionFactoryMock implements FunctionFactory {

    public static final ConfigOption<List<Record>> OPTION_COLLECTION =
            ConfigOptions.key("collection").<List<Record>>objectType().noDefaultValue();

    @Override
    public Function create() {
        return new AbstractFunction() {

            private transient List<Record> collection;

            @Override
            public void open(Context context) throws Exception {
                super.open(context);
                this.collection = context.get(OPTION_COLLECTION);
            }

            @Override
            public void process(GroupOffset groupOffset, Iterator<Records> iterator)
                    throws Exception {
                while (iterator.hasNext()) {
                    final Records records = iterator.next();
                    foreachRecord(
                            records,
                            (key, value, allHeaders) ->
                                    collection.add(new DefaultRecord(allHeaders, key, value)));
                }
                commit(groupOffset, null);
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public String identity() {
        return "collection_mock";
    }
}
