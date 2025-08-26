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

package org.zicat.tributary.source.logstash.base.test;

import org.zicat.tributary.source.logstash.base.Message;
import org.zicat.tributary.source.logstash.base.MessageFilter;

import java.util.Map;

/** DefaultMessageFilter. */
@SuppressWarnings("rawtypes")
public class DefaultMessageFilter implements MessageFilter<Object> {
    @Override
    public boolean filter(Message<Object> message) {
        final Map data = message.getData();
        data.remove("aa");
        return true;
    }
}
