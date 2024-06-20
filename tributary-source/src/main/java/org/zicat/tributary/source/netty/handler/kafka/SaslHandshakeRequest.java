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

/** SaslHandshakeRequest. */
public class SaslHandshakeRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<SaslHandshakeRequest> {
        private final SaslHandshakeRequestData data;

        public Builder(SaslHandshakeRequestData data) {
            super(ApiKeys.SASL_HANDSHAKE);
            this.data = data;
        }

        @Override
        public SaslHandshakeRequest build(short version) {
            return new SaslHandshakeRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final SaslHandshakeRequestData data;

    public SaslHandshakeRequest(SaslHandshakeRequestData data) {
        this(data, ApiKeys.SASL_HANDSHAKE.latestVersion());
    }

    public SaslHandshakeRequest(SaslHandshakeRequestData data, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        this.data = data;
    }

    public SaslHandshakeRequest(Struct struct, short version) {
        super(ApiKeys.SASL_HANDSHAKE, version);
        this.data = new SaslHandshakeRequestData(struct, version);
    }

    public SaslHandshakeRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        SaslHandshakeResponseData response = new SaslHandshakeResponseData();
        response.setErrorCode(ApiError.fromThrowable(e).error().code());
        return new SaslHandshakeResponse(response);
    }

    public static SaslHandshakeRequest parse(ByteBuffer buffer, short version) {
        return new SaslHandshakeRequest(
                ApiKeys.SASL_HANDSHAKE.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }
}
