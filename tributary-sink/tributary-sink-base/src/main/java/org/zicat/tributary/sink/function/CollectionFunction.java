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

package org.zicat.tributary.sink.function;

import org.zicat.tributary.channel.GroupOffset;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.records.Records;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** CollectionFunction. */
public class CollectionFunction extends AbstractFunction {

    private static final ConfigOption<Boolean> OPTION_CLEAR_BEFORE_CLOSE =
            ConfigOptions.key("clearBeforeClose")
                    .booleanType()
                    .description("whether clear received data before close")
                    .defaultValue(true);

    public final List<Records> history = new ArrayList<>();
    private transient boolean clearBeforeClose;

    @Override
    public void open(Context context) throws Exception {
        super.open(context);
        clearBeforeClose = context.get(OPTION_CLEAR_BEFORE_CLOSE);
    }

    @Override
    public void process(GroupOffset groupOffset, Iterator<Records> iterator) {
        while (iterator.hasNext()) {
            history.add(iterator.next());
        }
        commit(groupOffset, null);
    }

    @Override
    public void close() {
        if (clearBeforeClose) {
            history.clear();
        }
    }
}
