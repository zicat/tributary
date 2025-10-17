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

package org.zicat.tributary.common.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/** ConfigOption. */
public class ConfigOptions {

    private static final Map<String, ChronoUnit> LABEL_TO_UNIT_MAP =
            java.util.Collections.unmodifiableMap(initMap());

    public static ConfigOptionTypeBuilder key(String key) {
        return new ConfigOptionTypeBuilder(key);
    }

    /** ConfigOptionTypeBuilder. */
    @SuppressWarnings("unchecked")
    public static class ConfigOptionTypeBuilder {

        private final String key;

        public ConfigOptionTypeBuilder(String key) {
            this.key = key;
        }

        public final Builder<String> stringType() {
            return new Builder<>(key, Object::toString);
        }

        public final Builder<Integer> integerType() {
            return new Builder<>(key, s -> Integer.parseInt(s.toString()));
        }

        public final Builder<Long> longType() {
            return new Builder<>(key, s -> Long.parseLong(s.toString()));
        }

        public final Builder<Boolean> booleanType() {
            return new Builder<>(key, s -> Boolean.parseBoolean(s.toString()));
        }

        public final Builder<Duration> durationType() {
            return new Builder<>(
                    key, s -> s instanceof Duration ? (Duration) s : parseDuration(s.toString()));
        }

        public final Builder<MemorySize> memoryType() {
            return new Builder<>(
                    key,
                    s -> s instanceof MemorySize ? (MemorySize) s : MemorySize.parse(s.toString()));
        }

        public final Builder<PercentSize> percentType() {
            return new Builder<>(
                    key,
                    s ->
                            s instanceof PercentSize
                                    ? (PercentSize) s
                                    : PercentSize.parse(s.toString()));
        }

        public final <T extends Enum<T>> Builder<T> enumType(Class<T> enumClass) {
            return new Builder<>(key, s -> convertToEnum(s, enumClass));
        }

        public final <T> Builder<List<T>> listType(SplitHandler<T> handler) {
            return new Builder<>(key, handler::split);
        }

        public final <T> Builder<T> objectType() {
            return new Builder<>(key, o -> (T) o);
        }
    }

    /** SplitHandler. */
    public interface SplitHandler<T> {
        List<T> split(Object value);
    }

    /** StringSplitHandler. */
    @SuppressWarnings("unchecked")
    public static class StringSplitHandler implements SplitHandler<String> {

        private final String delimiter;

        public StringSplitHandler(String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public List<String> split(Object value) {
            if (value instanceof List) {
                return (List<String>) value;
            }
            return Arrays.stream(value.toString().split(delimiter))
                    .map(String::trim)
                    .filter(v -> !v.isEmpty())
                    .collect(Collectors.toList());
        }
    }

    public static final StringSplitHandler COMMA_SPLIT_HANDLER = new StringSplitHandler(",");

    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }
        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    /**
     * Builder.
     *
     * @param <T>
     */
    public static class Builder<T> {

        private final String key;
        private final Function<Object, T> valueConvert;
        private String description;

        public Builder(String key, Function<Object, T> valueConvert) {
            this.key = key;
            this.valueConvert = valueConvert;
        }

        public Builder<T> description(String description) {
            this.description = description;
            return this;
        }

        /**
         * set value.
         *
         * @param v v
         * @return ConfigOption
         */
        public ConfigOption<T> defaultValue(T v) {
            return new ConfigOption<>(key, valueConvert, v, description, true);
        }

        /**
         * set no default value.
         *
         * @return ConfigOption
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, valueConvert, null, description, false);
        }
    }

    /**
     * parse duration by text.
     *
     * @param text text
     * @return Duration
     */
    public static Duration parseDuration(String text) {
        if (text == null) {
            throw new IllegalArgumentException("text is null");
        }
        final String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("text is an empty string");
        }

        final int len = trimmed.length();
        int pos = 0;

        char current;
        while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
            pos++;
        }

        final String number = trimmed.substring(0, pos);
        final String unitLabel = trimmed.substring(pos).trim().toLowerCase(Locale.US);

        if (number.isEmpty()) {
            throw new NumberFormatException("text does not start with a number");
        }

        final long value;
        try {
            value = Long.parseLong(number);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "The value '"
                            + number
                            + "' cannot be re represented as 64bit number (numeric overflow).");
        }

        if (unitLabel.isEmpty()) {
            return Duration.of(value, ChronoUnit.MILLIS);
        }

        final ChronoUnit unit = LABEL_TO_UNIT_MAP.get(unitLabel);
        if (unit != null) {
            return Duration.of(value, unit);
        } else {
            throw new IllegalArgumentException(
                    "Time interval unit label '"
                            + unitLabel
                            + "' does not match any of the recognized units: "
                            + TimeUnit.getAllUnits());
        }
    }

    /**
     * init map.
     *
     * @return map
     */
    private static Map<String, ChronoUnit> initMap() {
        Map<String, ChronoUnit> labelToUnit = new HashMap<>();
        for (TimeUnit timeUnit : TimeUnit.values()) {
            for (String label : timeUnit.getLabels()) {
                labelToUnit.put(label, timeUnit.getUnit());
            }
        }
        return labelToUnit;
    }

    /** TimeUnit. */
    private enum TimeUnit {
        DAYS(ChronoUnit.DAYS, singular("d"), plural("day")),
        HOURS(ChronoUnit.HOURS, singular("h"), plural("hour")),
        MINUTES(ChronoUnit.MINUTES, singular("min"), singular("m"), plural("minute")),
        SECONDS(ChronoUnit.SECONDS, singular("s"), plural("sec"), plural("second")),
        MILLISECONDS(ChronoUnit.MILLIS, singular("ms"), plural("milli"), plural("millisecond")),
        MICROSECONDS(ChronoUnit.MICROS, singular("µs"), plural("micro"), plural("microsecond")),
        NANOSECONDS(ChronoUnit.NANOS, singular("ns"), plural("nano"), plural("nanosecond"));

        private static final String PLURAL_SUFFIX = "s";

        private final List<String> labels;

        private final ChronoUnit unit;

        TimeUnit(ChronoUnit unit, String[]... labels) {
            this.unit = unit;
            this.labels =
                    Arrays.stream(labels).flatMap(Arrays::stream).collect(Collectors.toList());
        }

        /**
         * @param label the original label
         * @return the singular format of the original label
         */
        private static String[] singular(String label) {
            return new String[] {label};
        }

        /**
         * @param label the original label
         * @return both the singular format and plural format of the original label
         */
        private static String[] plural(String label) {
            return new String[] {label, label + PLURAL_SUFFIX};
        }

        public List<String> getLabels() {
            return labels;
        }

        public ChronoUnit getUnit() {
            return unit;
        }

        public static String getAllUnits() {
            return Arrays.stream(TimeUnit.values())
                    .map(TimeUnit::createTimeUnitString)
                    .collect(Collectors.joining(", "));
        }

        private static String createTimeUnitString(TimeUnit timeUnit) {
            return timeUnit.name() + ": (" + String.join(" | ", timeUnit.getLabels()) + ")";
        }
    }
}
