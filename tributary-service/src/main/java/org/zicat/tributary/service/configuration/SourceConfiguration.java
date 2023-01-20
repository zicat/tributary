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

package org.zicat.tributary.service.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/** SourceConfiguration. */
@ConfigurationProperties
@Configuration
public class SourceConfiguration {

    Map<String, String> source;

    /**
     * get source config.
     *
     * @return source config map
     */
    public Map<String, String> getSource() {
        return source;
    }

    /**
     * set source config. for @ConfigurationProperties
     *
     * @param source source config map
     */
    public void setSource(Map<String, String> source) {
        this.source = source;
    }
}
