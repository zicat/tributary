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

package org.zicat.tributary.source.http;

import com.github.luben.zstd.Zstd;
import org.xerial.snappy.Snappy;
import static org.zicat.tributary.common.IOUtils.decompressionGZip;

import java.io.IOException;

/** Extraction. */
public enum Extraction {
    NONE("none") {
        @Override
        public byte[] extract(byte[] body) {
            return body;
        }
    },

    GZIP("gzip") {
        @Override
        public byte[] extract(byte[] body) throws IOException {
            return decompressionGZip(body);
        }
    },

    SNAPPY("snappy") {
        @Override
        public byte[] extract(byte[] body) throws IOException {
            return Snappy.uncompress(body);
        }
    },

    ZSTD("zstd") {
        @Override
        public byte[] extract(byte[] body) {
            return Zstd.decompress(body, (int) Zstd.decompressedSize(body));
        }
    };

    private final String name;

    Extraction(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * encode body to target map.
     *
     * @param body body
     * @return encoded map list
     */
    public abstract byte[] extract(byte[] body) throws IOException;

    public static Extraction getExtraction(String name) {
        if (name == null || name.trim().isEmpty()) {
            return NONE;
        }
        for (Extraction extraction : values()) {
            if (extraction.getName().equalsIgnoreCase(name)) {
                return extraction;
            }
        }
        throw new IllegalArgumentException("Unsupported extraction: " + name);
    }
}
