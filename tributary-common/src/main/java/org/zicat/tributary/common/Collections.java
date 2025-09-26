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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/** Collections. */
public class Collections {

    /**
     * copy iterator to new iterator.
     *
     * @param iterator iterator
     * @param <T> type
     * @return new iterator
     */
    public static <T> Iterator<T> copy(Iterator<T> iterator) {

        if (iterator == null) {
            return null;
        }
        final Collection<T> copy = new LinkedList<>();
        while (iterator.hasNext()) {
            copy.add(iterator.next());
        }
        return copy.iterator();
    }
}
