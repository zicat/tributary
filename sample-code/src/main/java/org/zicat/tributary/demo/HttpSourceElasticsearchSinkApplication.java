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

package org.zicat.tributary.demo;

import org.zicat.tributary.server.Starter;
import org.zicat.tributary.server.config.PropertiesLoader;

import java.io.IOException;

/** LineSourceElasticsearchSinkApplication. */
public class HttpSourceElasticsearchSinkApplication {

    private static final String ACTIVE_PROFILE = "http-source-elasticsearch-sink";

    public static void main(String[] args) throws IOException, InterruptedException {
        /*
            curl -X POST http://localhost:8200?topic=my_topic    \
                -H "Content-Type: application/json; charset=UTF-8"              \
                -H "my_records_header: hv1"                                     \
                -d '[{"key":"123123","value":"{\"name\":\"n1\",\"ts\":123123}","headers":{"header1":"value1","header2":"value2"}}]' -i
        */
        try (Starter starter = new Starter(new PropertiesLoader(ACTIVE_PROFILE).load())) {
            starter.start();
        }
    }
}
