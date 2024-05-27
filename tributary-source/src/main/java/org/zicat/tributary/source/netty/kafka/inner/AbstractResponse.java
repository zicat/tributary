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

package org.zicat.tributary.source.netty.kafka.inner;

import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** AbstractResponse. */
public abstract class AbstractResponse implements AbstractRequestResponse {
    public static final int DEFAULT_THROTTLE_TIME = 0;

    protected Send toSend(String destination, ResponseHeader header, short apiVersion) {
        return new NetworkSend(
                destination, RequestUtils.serialize(header.toStruct(), toStruct(apiVersion)));
    }

    /**
     * Visible for testing, typically {@link #toSend(String, ResponseHeader, short)} should be used
     * instead.
     */
    public ByteBuffer serialize(short version, ResponseHeader responseHeader) {
        return RequestUtils.serialize(responseHeader.toStruct(), toStruct(version));
    }

    /**
     * Visible for testing, typically {@link #toSend(String, ResponseHeader, short)} should be used
     * instead.
     */
    public ByteBuffer serialize(ApiKeys apiKey, short version, int correlationId) {
        ResponseHeader header =
                new ResponseHeader(correlationId, apiKey.responseHeaderVersion(version));
        return RequestUtils.serialize(header.toStruct(), toStruct(version));
    }

    public abstract Map<Errors, Integer> errorCounts();

    protected Map<Errors, Integer> errorCounts(Errors error) {
        return Collections.singletonMap(error, 1);
    }

    protected Map<Errors, Integer> errorCounts(Collection<Errors> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (Errors error : errors) updateErrorCounts(errorCounts, error);
        return errorCounts;
    }

    protected Map<Errors, Integer> apiErrorCounts(Map<?, ApiError> errors) {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        for (ApiError apiError : errors.values()) updateErrorCounts(errorCounts, apiError.error());
        return errorCounts;
    }

    protected void updateErrorCounts(Map<Errors, Integer> errorCounts, Errors error) {
        Integer count = errorCounts.get(error);
        errorCounts.put(error, count == null ? 1 : count + 1);
    }

    protected abstract Struct toStruct(short version);

    public static AbstractResponse parseResponse(ApiKeys apiKey, Struct struct, short version) {
        switch (apiKey) {
            case PRODUCE:
                return new ProduceResponse(struct);
            case API_VERSIONS:
                return ApiVersionsResponse.fromStruct(struct, version);
            case METADATA:
                return new MetadataResponse(struct, version);
            case SASL_HANDSHAKE:
                return new SaslHandshakeResponse(struct, version);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateResponse(struct, version);
            default:
                throw new AssertionError(
                        String.format(
                                "ApiKey %s is not currently handled in `parseResponse`, the "
                                        + "code should be updated to do so.",
                                apiKey));
        }
    }

    /**
     * Returns whether or not client should throttle upon receiving a response of the specified
     * version with a non-zero throttle time. Client-side throttling is needed when communicating
     * with a newer version of broker which, on quota violation, sends out responses before
     * throttling.
     */
    public boolean shouldClientThrottle(short version) {
        return false;
    }

    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public String toString(short version) {
        return toStruct(version).toString();
    }
}
