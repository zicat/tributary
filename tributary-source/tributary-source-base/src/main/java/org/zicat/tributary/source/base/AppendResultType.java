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

package org.zicat.tributary.source.base;

import org.zicat.tributary.channel.Segment.AppendResult;

import java.io.IOException;

/** AppendResultType. */
public enum AppendResultType {
    BLOCK("block") {
        @Override
        public void dealAppendResult(AppendResult appendResult) {
            if (appendResult == null) {
                return;
            }
            if (!appendResult.appended()) {
                throw new IllegalStateException("append block failed");
            }
        }
    },

    STORAGE("storage") {
        @Override
        public void dealAppendResult(AppendResult appendResult)
                throws IOException, InterruptedException {
            if (appendResult == null) {
                return;
            }
            if (!appendResult.appended()) {
                throw new IllegalStateException("append block failed");
            }
            appendResult.await2Storage();
        }
    };

    private final String name;

    AppendResultType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * deal append result.
     *
     * @param appendResult appendResult
     */
    public abstract void dealAppendResult(AppendResult appendResult)
            throws IOException, InterruptedException;
}
