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
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.RecordBatch;
import org.zicat.tributary.source.netty.handler.kafka.ApiVersionsResponseData.ApiVersionsResponseKey;
import org.zicat.tributary.source.netty.handler.kafka.ApiVersionsResponseData.ApiVersionsResponseKeyCollection;

import java.nio.ByteBuffer;
import java.util.Map;

/** ApiVersionsResponse. */
public class ApiVersionsResponse extends AbstractResponse {

    public static final ApiVersionsResponse DEFAULT_API_VERSIONS_RESPONSE =
            createApiVersionsResponse(DEFAULT_THROTTLE_TIME, RecordBatch.CURRENT_MAGIC_VALUE);

    public final ApiVersionsResponseData data;

    public ApiVersionsResponse(ApiVersionsResponseData data) {
        this.data = data;
    }

    public ApiVersionsResponse(Struct struct) {
        this(
                new ApiVersionsResponseData(
                        struct, (short) (ApiVersionsResponseData.SCHEMAS.length - 1)));
    }

    public ApiVersionsResponse(Struct struct, short version) {
        this(new ApiVersionsResponseData(struct, version));
    }

    @Override
    protected Struct toStruct(short version) {
        return this.data.toStruct(version);
    }

    public ApiVersionsResponseKey apiVersion(short apiKey) {
        return data.apiKeys().find(apiKey);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(this.data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return this.data.throttleTimeMs();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it into a Struct which means that the version received by this
        // method is not necessary the real one. It may be version 0 as well.
        int prev = buffer.position();
        try {
            return new ApiVersionsResponse(
                    new ApiVersionsResponseData(new ByteBufferAccessor(buffer), version));
        } catch (RuntimeException e) {
            buffer.position(prev);
            if (version != 0) {
                return new ApiVersionsResponse(
                        new ApiVersionsResponseData(new ByteBufferAccessor(buffer), (short) 0));
            } else {
                throw e;
            }
        }
    }

    public static ApiVersionsResponse fromStruct(Struct struct, short version) {
        // Fallback to version 0 for ApiVersions response. If a client sends an ApiVersionsRequest
        // using a version higher than that supported by the broker, a version 0 response is sent
        // to the client indicating UNSUPPORTED_VERSION. When the client receives the response, it
        // falls back while parsing it into a Struct which means that the version received by this
        // method is not necessary the real one. It may be version 0 as well.
        try {
            return new ApiVersionsResponse(struct, version);
        } catch (SchemaException e) {
            if (version != 0) {
                return new ApiVersionsResponse(struct, (short) 0);
            } else {
                throw e;
            }
        }
    }

    public static ApiVersionsResponse apiVersionsResponse(int throttleTimeMs, byte maxMagic) {
        if (maxMagic == RecordBatch.CURRENT_MAGIC_VALUE
                && throttleTimeMs == DEFAULT_THROTTLE_TIME) {
            return DEFAULT_API_VERSIONS_RESPONSE;
        }
        return createApiVersionsResponse(throttleTimeMs, maxMagic);
    }

    public static ApiVersionsResponse createApiVersionsResponse(
            int throttleTimeMs, final byte minMagic) {
        ApiVersionsResponseKeyCollection apiKeys = new ApiVersionsResponseKeyCollection();
        for (ApiKeys apiKey : ApiKeys.values()) {
            if (apiKey.minRequiredInterBrokerMagic <= minMagic) {
                apiKeys.add(
                        new ApiVersionsResponseKey()
                                .setApiKey(apiKey.id)
                                .setMinVersion(apiKey.oldestVersion())
                                .setMaxVersion(apiKey.latestVersion()));
            }
        }

        ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setThrottleTimeMs(throttleTimeMs);
        data.setErrorCode(Errors.NONE.code());
        data.setApiKeys(apiKeys);

        return new ApiVersionsResponse(data);
    }
}
