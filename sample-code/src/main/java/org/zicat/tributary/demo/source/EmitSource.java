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

package org.zicat.tributary.demo.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.Threads;
import org.zicat.tributary.common.records.Records;
import org.zicat.tributary.common.records.RecordsUtils;
import org.zicat.tributary.source.RecordsChannel;
import org.zicat.tributary.source.Source;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/** EmitSource. */
public class EmitSource implements Source {

    private static final Logger LOG = LoggerFactory.getLogger(EmitSource.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Random random = new Random();
    private final Runnable task;
    private Thread t;

    public EmitSource(RecordsChannel channel) {
        this.task =
                () -> {
                    while (!closed.get()) {
                        final Records records =
                                RecordsUtils.createStringRecords(
                                        channel.topic(), new SimpleDateFormat().format(new Date()));
                        try {
                            channel.append(random.nextInt(channel.partition()), records);
                        } catch (IOException e) {
                            LOG.warn("append fail", e);
                        } finally {
                            Threads.sleepQuietly(1000);
                        }
                    }
                };
    }

    @Override
    public void start() {
        t = new Thread(task);
        t.start();
    }

    @Override
    public String sourceId() {
        return String.valueOf(this.hashCode());
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            Threads.joinQuietly(t);
        }
    }
}
