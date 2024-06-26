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

package org.zicat.tributary.source.netty.handler.kafka;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/** ResponseHeader. */
public class ResponseHeader implements AbstractRequestResponse {
    private final ResponseHeaderData data;
    private final short headerVersion;

    public ResponseHeader(Struct struct, short headerVersion) {
        this(new ResponseHeaderData(struct, headerVersion), headerVersion);
    }

    public ResponseHeader(int correlationId, short headerVersion) {
        this(new ResponseHeaderData().setCorrelationId(correlationId), headerVersion);
    }

    public ResponseHeader(ResponseHeaderData data, short headerVersion) {
        this.data = data;
        this.headerVersion = headerVersion;
    }

    public int sizeOf() {
        return toStruct().sizeOf();
    }

    public Struct toStruct() {
        return data.toStruct(headerVersion);
    }

    public int correlationId() {
        return this.data.correlationId();
    }

    public short headerVersion() {
        return headerVersion;
    }

    public ResponseHeaderData data() {
        return data;
    }

    public static ResponseHeader parse(ByteBuffer buffer, short headerVersion) {
        return new ResponseHeader(
                new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion),
                headerVersion);
    }
}
