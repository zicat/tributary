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

package org.zicat.tributary.sink.elasticsearch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.zicat.tributary.channel.Offset;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** DefaultActionListener. */
public class DefaultActionListener implements ActionListener<BulkResponse> {

    private static final Exception NO_EXCEPTION = new Exception();

    private final Offset offset;
    private final AtomicReference<Exception> state;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public DefaultActionListener(Offset offset) {
        this.offset = offset;
        this.state = new AtomicReference<>();
    }

    @Override
    public void onResponse(BulkResponse response) {
        if (!response.hasFailures()) {
            updateState(NO_EXCEPTION);
            return;
        }
        for (BulkItemResponse item : response.getItems()) {
            if (!item.isFailed()) {
                continue;
            }
            final String index = item.getIndex();
            final String id = item.getId();
            final String error = item.getFailureMessage();
            updateState(
                    new IllegalStateException(
                            "Failed to index document id: "
                                    + id
                                    + ", index: "
                                    + index
                                    + ", error: "
                                    + error));
            return;
        }
        updateState(NO_EXCEPTION);
    }

    @Override
    public void onFailure(Exception e) {
        updateState(e);
    }

    /** offset. */
    public Offset offset() {
        return offset;
    }

    /**
     * update state.
     *
     * @param e e
     */
    private void updateState(Exception e) {
        state.set(e);
        countDownLatch.countDown();
    }

    /**
     * check is done.
     *
     * @return return true if done.
     */
    public boolean isDone() {
        return state.get() != null;
    }

    /**
     * await done.
     *
     * @param timeout timeout
     * @param unit unit
     * @return true if the count reached zero and false if the waiting time elapsed before the count
     *     reached zero
     * @throws InterruptedException InterruptedException
     */
    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        return countDownLatch.await(timeout, unit);
    }

    /**
     * get exception.
     *
     * @return exception.
     */
    public Exception exception() {
        final Exception e = state.get();
        if (e == null) {
            throw new IllegalStateException("ActionListener is not done.");
        }
        return e == NO_EXCEPTION ? null : e;
    }
}
