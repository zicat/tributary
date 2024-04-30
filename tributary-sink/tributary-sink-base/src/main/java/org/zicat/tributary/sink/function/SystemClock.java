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

package org.zicat.tributary.sink.function;

import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** SystemClock. default time zone is utc */
public class SystemClock implements Clock {

    private static final Map<String, DateTimeZone> ID_MAPPING = new ConcurrentHashMap<>();
    private static final Function<String, DateTimeZone> TIME_ZONE_FUNCTION =
            key -> DateTimeZone.forTimeZone(TimeZone.getTimeZone(key));

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public String currentTime(String pattern, String timeZoneId) {
        return timeFormat(DateTimeUtils.currentTimeMillis(), pattern, timeZoneId);
    }

    /**
     * get time format.
     *
     * @param timeMillis timeMillis
     * @param pattern pattern
     * @param timeZoneId timeZoneId
     * @return string value
     */
    public static String timeFormat(long timeMillis, String pattern, String timeZoneId) {
        final DateTimeZone timeZone = ID_MAPPING.computeIfAbsent(timeZoneId, TIME_ZONE_FUNCTION);
        return new LocalDateTime(timeMillis, timeZone).toString(pattern);
    }
}
