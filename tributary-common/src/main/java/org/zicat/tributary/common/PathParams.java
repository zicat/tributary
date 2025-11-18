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

package org.zicat.tributary.common;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/** PathParams. */
public class PathParams {

    private static final String ENCODE = StandardCharsets.UTF_8.name();
    private final Map<String, String> params;
    private final String path;

    private PathParams(String u) throws URISyntaxException, UnsupportedEncodingException {
        final URI uri = new URI(u);
        this.path = uri.getPath();
        this.params = params(uri);
    }

    public static PathParams create(String u) {
        try {
            return new PathParams(u);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * parse uri for params.
     *
     * @param uri uri
     * @return params
     */
    private static Map<String, String> params(URI uri) throws UnsupportedEncodingException {
        final Map<String, String> params = new HashMap<>();
        if (uri.getQuery() == null) {
            return params;
        }
        final String query = uri.getQuery();
        if (query != null) {
            final String[] pairs = query.split("&");
            for (String pair : pairs) {
                final int idx = pair.indexOf("=");
                final String key = URLDecoder.decode(pair.substring(0, idx), ENCODE);
                final String value = URLDecoder.decode(pair.substring(idx + 1), ENCODE);
                params.put(key, value);
            }
        }
        return params;
    }

    public Map<String, String> params() {
        return params;
    }

    public String path() {
        return path;
    }
}
