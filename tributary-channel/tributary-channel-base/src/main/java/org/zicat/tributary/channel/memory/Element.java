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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.channel.RecordsOffset;

/** Element. */
public class Element {
    private final byte[] record;
    private final int offset;
    private final int length;
    private final RecordsOffset recordsOffset;

    Element(byte[] record, int offset, int length, RecordsOffset recordsOffset) {
        this.record = record;
        this.offset = offset;
        this.length = length;
        this.recordsOffset = recordsOffset;
    }

    public byte[] record() {
        return record;
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    public RecordsOffset recordsOffset() {
        return recordsOffset;
    }
}
