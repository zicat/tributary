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

import org.zicat.tributary.queue.RecordsOffset;

/** AbstractFunction. */
public abstract class AbstractFunction implements Function {

    public static final String FLUSH_MILL = "flushMill";
    public static final String CLOCK = "clock";

    protected Context context;
    protected Clock clock;

    private RecordsOffset committableOffset;
    protected long flushMill;
    private Long preFlushTime;

    @Override
    public void open(Context context) {
        this.context = context;
        this.committableOffset = context.startRecordsOffset();
        this.flushMill = context.getCustomProperty(FLUSH_MILL, 60000);
        final Object clock = context.getCustomProperty(CLOCK);
        this.clock = clock instanceof Clock ? (Clock) clock : new SystemClock();
    }

    @Override
    public final RecordsOffset committableOffset() {
        return committableOffset;
    }

    /**
     * get context.
     *
     * @return context
     */
    public final Context context() {
        return context;
    }

    /**
     * check whether flushable.
     *
     * @return boolean
     */
    protected boolean committable() {
        final long current = clock.currentTimeMillis();
        if (preFlushTime == null) {
            preFlushTime = current;
        }
        final boolean flushable = current - preFlushTime >= flushMill;
        if (flushable) {
            preFlushTime = current;
        }
        return flushable;
    }

    /**
     * execute callback and persist offset.
     *
     * @param newCommittableOffset newCommittableOffset
     * @param callback callback
     */
    public final void flush(RecordsOffset newCommittableOffset, OnFlushCallback callback) {
        if (newCommittableOffset == null || !committable()) {
            return;
        }
        if (callback == null || callback.run()) {
            this.committableOffset = newCommittableOffset;
        }
    }

    /** OnFlushCallback. */
    public interface OnFlushCallback {

        /** run callback function before commit offset. */
        boolean run();
    }
}
