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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.protocol.types.Type.*;

/** ApiKeys. */
public enum ApiKeys {
    PRODUCE(0, "Produce", ProduceRequest.schemaVersions(), ProduceResponse.schemaVersions()),
    API_VERSIONS(
            18, "ApiVersions", ApiVersionsRequestData.SCHEMAS, ApiVersionsResponseData.SCHEMAS) {
        @Override
        public Struct parseResponse(short version, ByteBuffer buffer) {
            // Fallback to version 0 for ApiVersions response. If a client sends an
            // ApiVersionsRequest
            // using a version higher than that supported by the broker, a version 0 response is
            // sent
            // to the client indicating UNSUPPORTED_VERSION.
            return parseResponse(version, buffer, (short) 0);
        }
    },
    METADATA(3, "Metadata", MetadataRequestData.SCHEMAS, MetadataResponseData.SCHEMAS),
    SASL_HANDSHAKE(
            17,
            "SaslHandshake",
            SaslHandshakeRequestData.SCHEMAS,
            SaslHandshakeResponseData.SCHEMAS),

    SASL_AUTHENTICATE(
            36,
            "SaslAuthenticate",
            SaslAuthenticateRequestData.SCHEMAS,
            SaslAuthenticateResponseData.SCHEMAS);

    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values()) maxKey = Math.max(maxKey, key.id);
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values()) idToType[key.id] = key;
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    /** the permanent and immutable id of an API--this can't change ever */
    public final short id;

    /** an english description of the api--this is for debugging and can change */
    public final String name;

    /** indicates if this is a ClusterAction request used only by brokers */
    public final boolean clusterAction;

    /** indicates the minimum required inter broker magic required to support the API */
    public final byte minRequiredInterBrokerMagic;

    public final Schema[] requestSchemas;
    public final Schema[] responseSchemas;
    public final boolean requiresDelayedAllocation;

    ApiKeys(int id, String name, Schema[] requestSchemas, Schema[] responseSchemas) {
        this(id, name, false, requestSchemas, responseSchemas);
    }

    ApiKeys(
            int id,
            String name,
            boolean clusterAction,
            Schema[] requestSchemas,
            Schema[] responseSchemas) {
        this(id, name, clusterAction, RecordBatch.MAGIC_VALUE_V0, requestSchemas, responseSchemas);
    }

    ApiKeys(
            int id,
            String name,
            boolean clusterAction,
            byte minRequiredInterBrokerMagic,
            Schema[] requestSchemas,
            Schema[] responseSchemas) {
        if (id < 0) {
            throw new IllegalArgumentException("id must not be negative, id: " + id);
        }
        this.id = (short) id;
        this.name = name;
        this.clusterAction = clusterAction;
        this.minRequiredInterBrokerMagic = minRequiredInterBrokerMagic;

        if (requestSchemas.length != responseSchemas.length) {
            throw new IllegalStateException(
                    requestSchemas.length
                            + " request versions for api "
                            + name
                            + " but "
                            + responseSchemas.length
                            + " response versions.");
        }

        for (int i = 0; i < requestSchemas.length; ++i) {
            if (requestSchemas[i] == null) {
                throw new IllegalStateException(
                        "Request schema for api " + name + " for version " + i + " is null");
            }
            if (responseSchemas[i] == null) {
                throw new IllegalStateException(
                        "Response schema for api " + name + " for version " + i + " is null");
            }
        }

        boolean requestRetainsBufferReference = false;
        for (Schema requestVersionSchema : requestSchemas) {
            if (retainsBufferReference(requestVersionSchema)) {
                requestRetainsBufferReference = true;
                break;
            }
        }
        this.requiresDelayedAllocation = requestRetainsBufferReference;
        this.requestSchemas = requestSchemas;
        this.responseSchemas = responseSchemas;
    }

    public static ApiKeys forId(int id) {
        if (!hasId(id)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unexpected ApiKeys id `%s`, it should be between `%s` "
                                    + "and `%s` (inclusive)",
                            id, MIN_API_KEY, MAX_API_KEY));
        }
        return ID_TO_TYPE[id];
    }

    public static boolean hasId(int id) {
        return id >= MIN_API_KEY && id <= MAX_API_KEY;
    }

    public short latestVersion() {
        return (short) (requestSchemas.length - 1);
    }

    public short oldestVersion() {
        return 0;
    }

    public Schema requestSchema(short version) {
        return schemaFor(requestSchemas, version);
    }

    public Schema responseSchema(short version) {
        return schemaFor(responseSchemas, version);
    }

    public Struct parseRequest(short version, ByteBuffer buffer) {
        return requestSchema(version).read(buffer);
    }

    public Struct parseResponse(short version, ByteBuffer buffer) {
        return responseSchema(version).read(buffer);
    }

    protected Struct parseResponse(short version, ByteBuffer buffer, short fallbackVersion) {
        int bufferPosition = buffer.position();
        try {
            return responseSchema(version).read(buffer);
        } catch (SchemaException e) {
            if (version != fallbackVersion) {
                buffer.position(bufferPosition);
                return responseSchema(fallbackVersion).read(buffer);
            } else {
                throw e;
            }
        }
    }

    private Schema schemaFor(Schema[] versions, short version) {
        if (!isVersionSupported(version)) {
            throw new IllegalArgumentException(
                    "Invalid version for API key " + this + ": " + version);
        }
        return versions[version];
    }

    public boolean isVersionSupported(short apiVersion) {
        return apiVersion >= oldestVersion() && apiVersion <= latestVersion();
    }

    public short requestHeaderVersion(short apiVersion) {
        return ApiMessageType.fromApiKey(id).requestHeaderVersion(apiVersion);
    }

    public short responseHeaderVersion(short apiVersion) {
        return ApiMessageType.fromApiKey(id).responseHeaderVersion(apiVersion);
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Name</th>\n");
        b.append("<th>Key</th>\n");
        b.append("</tr>");
        for (ApiKeys key : ApiKeys.values()) {
            b.append("<tr>\n");
            b.append("<td>");
            b.append("<a href=\"#The_Messages_" + key.name + "\">" + key.name + "</a>");
            b.append("</td>");
            b.append("<td>");
            b.append(key.id);
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }

    private static boolean retainsBufferReference(Schema schema) {
        final AtomicBoolean hasBuffer = new AtomicBoolean(false);
        Schema.Visitor detector =
                new Schema.Visitor() {
                    @Override
                    public void visit(Type field) {
                        if (field == BYTES
                                || field == NULLABLE_BYTES
                                || field == RECORDS
                                || field == COMPACT_BYTES
                                || field == COMPACT_NULLABLE_BYTES) {
                            hasBuffer.set(true);
                        }
                    }
                };
        schema.walk(detector);
        return hasBuffer.get();
    }
}
