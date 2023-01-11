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

package org.zicat.tributary.queue;

/** RecordsResultSetImpl. @NotThreadSafe */
public final class RecordsResultSetImpl implements RecordsResultSet {

    final BufferRecordsOffset nextRecordsOffset;
    byte[] nextData;

    RecordsResultSetImpl(BufferRecordsOffset nextRecordsOffset) {
        this.nextRecordsOffset = nextRecordsOffset;
        next();
    }

    @Override
    public final RecordsOffset nexRecordsOffset() {
        return nextRecordsOffset;
    }

    @Override
    public final boolean hasNext() {
        return nextData != null;
    }

    @Override
    public final byte[] next() {
        final byte[] result = this.nextData;
        this.nextData = nextRecordsOffset.readNext();
        return result;
    }

    @Override
    public final long readBytes() {
        return nextRecordsOffset.readBytes();
    }
}
