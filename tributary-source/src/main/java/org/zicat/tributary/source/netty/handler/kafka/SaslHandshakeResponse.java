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

import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** SaslHandshakeResponse. */
public class SaslHandshakeResponse extends AbstractResponse {

    private final SaslHandshakeResponseData data;

    public SaslHandshakeResponse(SaslHandshakeResponseData data) {
        this.data = data;
    }

    public SaslHandshakeResponse(Struct struct, short version) {
        this.data = new SaslHandshakeResponseData(struct, version);
    }

    /*
     * Possible error codes:
     *   UNSUPPORTED_SASL_MECHANISM(33): Client mechanism not enabled in server
     *   ILLEGAL_SASL_STATE(34) : Invalid request during SASL handshake
     */
    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    @Override
    public Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public List<String> enabledMechanisms() {
        return data.mechanisms();
    }

    public static SaslHandshakeResponse parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeResponse(
                ApiKeys.SASL_HANDSHAKE.parseResponse(version, buffer), version);
    }
}
