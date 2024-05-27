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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

/** AbstractRequest. */
public abstract class AbstractRequest implements AbstractRequestResponse {

    public abstract static class Builder<T extends AbstractRequest> {
        private final ApiKeys apiKey;
        private final short oldestAllowedVersion;
        private final short latestAllowedVersion;

        /** Construct a new builder which allows any supported version */
        public Builder(ApiKeys apiKey) {
            this(apiKey, apiKey.oldestVersion(), apiKey.latestVersion());
        }

        /** Construct a new builder which allows only a specific version */
        public Builder(ApiKeys apiKey, short allowedVersion) {
            this(apiKey, allowedVersion, allowedVersion);
        }

        /** Construct a new builder which allows an inclusive range of versions */
        public Builder(ApiKeys apiKey, short oldestAllowedVersion, short latestAllowedVersion) {
            this.apiKey = apiKey;
            this.oldestAllowedVersion = oldestAllowedVersion;
            this.latestAllowedVersion = latestAllowedVersion;
        }

        public ApiKeys apiKey() {
            return apiKey;
        }

        public short oldestAllowedVersion() {
            return oldestAllowedVersion;
        }

        public short latestAllowedVersion() {
            return latestAllowedVersion;
        }

        public T build() {
            return build(latestAllowedVersion());
        }

        public abstract T build(short version);
    }

    private final short version;
    public final ApiKeys api;

    public AbstractRequest(ApiKeys api, short version) {
        if (!api.isVersionSupported(version)) {
            throw new UnsupportedVersionException(
                    "The " + api + " protocol does not support version " + version);
        }
        this.version = version;
        this.api = api;
    }

    /** Get the version of this AbstractRequest object. */
    public short version() {
        return version;
    }

    public Send toSend(String destination, RequestHeader header) {
        return new NetworkSend(destination, serialize(header));
    }

    /** Use with care, typically {@link #toSend(String, RequestHeader)} should be used instead. */
    public ByteBuffer serialize(RequestHeader header) {
        return RequestUtils.serialize(header.toStruct(), toStruct());
    }

    protected abstract Struct toStruct();

    public String toString(boolean verbose) {
        return toStruct().toString();
    }

    @Override
    public final String toString() {
        return toString(true);
    }

    /** Get an error response for a request */
    public AbstractResponse getErrorResponse(Throwable e) {
        return getErrorResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, e);
    }

    /**
     * Get an error response for a request with specified throttle time in the response if
     * applicable
     */
    public abstract AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e);

    /**
     * Get the error counts corresponding to an error response. This is overridden for requests
     * where response may be null (e.g produce with acks=0).
     */
    public Map<Errors, Integer> errorCounts(Throwable e) {
        AbstractResponse response = getErrorResponse(0, e);
        if (response == null) {
            throw new IllegalStateException(
                    "Error counts could not be obtained for request " + this);
        } else {
            return response.errorCounts();
        }
    }

    /** Factory method for getting a request object based on ApiKey ID and a version */
    public static AbstractRequest parseRequest(ApiKeys apiKey, short apiVersion, Struct struct) {
        switch (apiKey) {
            case API_VERSIONS:
                return new ApiVersionsRequest(struct, apiVersion);
            case PRODUCE:
                return new ProduceRequest(struct, apiVersion);
            case METADATA:
                return new MetadataRequest(struct, apiVersion);
            case SASL_HANDSHAKE:
                return new SaslHandshakeRequest(struct, apiVersion);
            case SASL_AUTHENTICATE:
                return new SaslAuthenticateRequest(struct, apiVersion);

            default:
                throw new AssertionError(
                        String.format(
                                "ApiKey %s is not currently handled in `parseRequest`, the "
                                        + "code should be updated to do so.",
                                apiKey));
        }
    }
}
