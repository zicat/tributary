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

package org.zicat.tributary.source.utils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** SourceHeaders. */
public class SourceHeaders {

    public static final String HEADER_KEY_REC_TS = "_rec_ts";
    public static final String HEADER_KEY_AUTH_USER = "_auth_user";

    /**
     * source headers.
     *
     * @param receivedTs receivedTs
     * @param authUser authUser
     * @return headers.
     */
    public static Map<String, byte[]> sourceHeaders(int receivedTs, String authUser) {
        final Map<String, byte[]> result = new HashMap<>();
        result.put(HEADER_KEY_REC_TS, String.valueOf(receivedTs).getBytes(StandardCharsets.UTF_8));
        if (authUser != null) {
            result.put(HEADER_KEY_AUTH_USER, authUser.getBytes());
        }
        return result;
    }

    /**
     * source headers.
     *
     * @param receivedTs receivedTs
     * @return headers
     */
    public static Map<String, byte[]> sourceHeaders(int receivedTs) {
        return sourceHeaders(receivedTs, null);
    }
}
