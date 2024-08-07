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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Offset;
import org.zicat.tributary.common.records.Records;

import java.util.Iterator;

/** dummy data function do nothing. */
public class DummyFunction extends AbstractFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DummyFunction.class);

    @Override
    public void process(Offset offset, Iterator<Records> iterator) {
        commit(offset);
    }

    @Override
    public void close() {
        LOG.info("Dummy data function closed");
    }
}
