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

package org.zicat.tributary.service.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.channel.Channel;
import org.zicat.tributary.service.TributaryServiceApplication;

import java.util.Random;

/** DefaultSourceFunction. */
public class DefaultSourceFunction implements SourceFunction {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultSourceFunction.class);
    private final Random random = new Random(System.currentTimeMillis());

    @Override
    public void process(Channel channel, byte[] data) throws Exception {
        final int partition = random.nextInt(channel.partition());
        channel.append(partition, data);
    }

    @Override
    public void exception(Channel channel, byte[] data, Exception e) {
        LOG.error("append data error", e);
        TributaryServiceApplication.shutdown();
    }
}
