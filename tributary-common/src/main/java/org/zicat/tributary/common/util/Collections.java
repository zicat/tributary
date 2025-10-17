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

package org.zicat.tributary.common.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Collections. */
public class Collections {

    /**
     * copy iterator to new iterator.
     *
     * @param iterator iterator
     * @param <T> type
     * @return new iterator
     */
    public static <T> Iterator<T> deepCopy(Iterator<T> iterator) {

        if (iterator == null) {
            return null;
        }
        final Collection<T> copy = new LinkedList<>();
        while (iterator.hasNext()) {
            copy.add(iterator.next());
        }
        return copy.iterator();
    }

    /**
     * sum double map.
     *
     * @param stream stream
     * @return merged map
     * @param <K> KEY
     */
    public static <K> Map<K, Double> sumDouble(Stream<Map<K, Double>> stream) {
        return stream.flatMap(m -> m.entrySet().stream())
                .collect(
                        Collectors.groupingBy(
                                Map.Entry::getKey, Collectors.summingDouble(Map.Entry::getValue)));
    }

    /**
     * concat two list.
     *
     * @param list1 list1
     * @param list2 list2
     * @return new list
     * @param <T> T
     */
    public static <T> List<T> concat(List<T> list1, List<T> list2) {
        if (list1 == null || list1.isEmpty()) {
            return list2;
        }
        if (list2 == null || list2.isEmpty()) {
            return list1;
        }
        final List<T> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }
}
