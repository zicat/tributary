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

import java.util.Objects;

/** PercentileSize. */
public class PercentSize implements java.io.Serializable, Comparable<PercentSize> {

    // from 0-100
    private final double percent;

    public PercentSize(double percent) {
        if (percent < 0 || percent > 100) {
            throw new IllegalArgumentException("Percent size must be between 0 and 100");
        }
        this.percent = percent;
    }

    private static double parseText(String text) {
        if (text == null) {
            throw new NullPointerException("Percent size text is null");
        }
        text = text.trim();
        if (text.endsWith("%")) {
            String percentText = text.substring(0, text.length() - 1);
            return Double.parseDouble(percentText.trim());
        } else {
            throw new IllegalArgumentException("Percent size must end with %");
        }
    }

    /**
     * Parses the given string as PercentSize, like 59%, 50 %.
     *
     * @param text The string to parse
     * @return The parsed MemorySize
     * @throws IllegalArgumentException Thrown, if the expression cannot be parsed.
     */
    public static PercentSize parse(String text) throws IllegalArgumentException {
        return new PercentSize(parseText(text));
    }

    /**
     * Get percent value.
     *
     * @return percent value
     */
    public double getPercent() {
        return percent;
    }

    @Override
    public int compareTo(PercentSize o) {
        return Double.compare(this.percent, o.percent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PercentSize)) {
            return false;
        }
        PercentSize that = (PercentSize) o;
        return percent == that.percent;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(percent);
    }
}
