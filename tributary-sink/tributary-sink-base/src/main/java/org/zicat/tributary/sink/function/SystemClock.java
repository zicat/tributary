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
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.chrono.ISOChronology;

/** SystemClock. default time zone is utc */
public class SystemClock implements Clock {

    private final ISOChronology isoChronology;

    public SystemClock(DateTimeZone timeZone) {
        this.isoChronology = ISOChronology.getInstance(timeZone);
    }

    public SystemClock() {
        this(DateTimeZone.UTC);
    }

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    @Override
    public String currentTime(String pattern) {
        return new LocalDateTime(DateTimeUtils.currentTimeMillis(), isoChronology)
                .toString(pattern);
    }

    @Override
    public String today(String format) {
        return new LocalDate(DateTimeUtils.currentTimeMillis(), isoChronology).toString(format);
    }

    @Override
    public String tomorrow(String format) {
        return new LocalDate(DateTimeUtils.currentTimeMillis(), isoChronology)
                .plusDays(1)
                .toString(format);
    }

    @Override
    public int secondFromToday() {
        return new LocalDateTime(DateTimeUtils.currentTimeMillis(), isoChronology).getMillisOfDay()
                / 1000;
    }
}
