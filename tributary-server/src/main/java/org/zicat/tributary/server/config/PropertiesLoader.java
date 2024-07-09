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

package org.zicat.tributary.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** PropertiesLoader. */
public class PropertiesLoader {

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);
    public static final String PROPERTIES_NAME_TEMPLATE = "application%s.properties";
    public static final String ACTIVE_PROFILES_PROPERTY_NAME = "profiles.active";
    public static final String DEFAULT_PROFILES_PROPERTY_NAME = "profiles.default";

    private final String fileName;

    public PropertiesLoader(String profile) {
        this.fileName =
                String.format(PROPERTIES_NAME_TEMPLATE, profile == null ? "" : "-" + profile);
    }

    public PropertiesLoader() {
        this(getCurrentProfile());
    }

    /**
     * get active profile.
     *
     * @return profile
     */
    public static String getCurrentProfile() {
        final String profile = System.getProperty(ACTIVE_PROFILES_PROPERTY_NAME);
        if (profile != null) {
            return profile;
        }
        return System.getProperty(DEFAULT_PROFILES_PROPERTY_NAME);
    }

    /**
     * set active profile.
     *
     * @param profile profile
     */
    public static void setActiveProfile(String profile) {
        System.setProperty(ACTIVE_PROFILES_PROPERTY_NAME, profile);
    }

    /**
     * load properties.
     *
     * @return properties
     * @throws IOException IOException
     */
    public Properties load() throws IOException {
        final Properties properties = new Properties();
        try (InputStream in =
                Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName)) {
            properties.load(in);
            LOG.info("load properties from {}", fileName);
            return properties;
        }
    }
}
