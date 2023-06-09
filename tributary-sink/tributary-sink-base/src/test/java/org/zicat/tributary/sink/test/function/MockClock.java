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

package org.zicat.tributary.sink.test.function;

import org.zicat.tributary.sink.function.Clock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/** MockClock. */
public class MockClock implements Clock {

    private long currentTimeMillis;

    public void setCurrentTimeMillis(long currentTimeMillis) {
        this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    public long currentTimeMillis() {
        return currentTimeMillis;
    }

    @Override
    public String currentTime(String pattern, String timeZoneId) {
        final SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZoneId));
        return dateFormat.format(new Date(currentTimeMillis));
    }
}
