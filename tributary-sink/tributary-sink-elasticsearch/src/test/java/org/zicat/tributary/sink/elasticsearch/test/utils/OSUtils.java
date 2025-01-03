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

package org.zicat.tributary.sink.elasticsearch.test.utils;

/** OSUtils. */
public class OSUtils {

    /** OSType. */
    public enum OSType {
        WINDOWS,
        MAC,
        LINUX,
        OTHER
    }

    public static OSType osType() {
        final String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("mac")) {
            return OSType.MAC;
        } else if (osName.contains("nix") || osName.contains("nux")) {
            return OSType.LINUX;
        } else if (osName.contains("win")) {
            return OSType.WINDOWS;
        } else {
            return OSType.OTHER;
        }
    }
}
