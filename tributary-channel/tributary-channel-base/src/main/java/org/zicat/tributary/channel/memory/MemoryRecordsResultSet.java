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

package org.zicat.tributary.channel.memory;

import org.zicat.tributary.channel.RecordsOffset;
import org.zicat.tributary.channel.RecordsResultSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** MemoryRecordsResultSet. */
public class MemoryRecordsResultSet implements RecordsResultSet {

    final List<Element> elements;
    final Iterator<Element> it;
    final RecordsOffset nextRecordsOffset;

    public MemoryRecordsResultSet(RecordsOffset nextRecordsOffset) {
        this(null, nextRecordsOffset);
    }

    public MemoryRecordsResultSet(List<Element> elements, RecordsOffset nextRecordsOffset) {
        this.elements = elements;
        this.it = elements == null ? Collections.emptyIterator() : elements.listIterator();
        this.nextRecordsOffset = nextRecordsOffset;
    }

    @Override
    public RecordsOffset nexRecordsOffset() {
        return nextRecordsOffset;
    }

    @Override
    public long readBytes() {
        return elements == null ? 0 : elements.stream().mapToInt(Element::length).sum();
    }

    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    @Override
    public byte[] next() {
        final Element element = it.next();
        if (element == null) {
            return null;
        }
        if (element.offset() == 0 && element.length() == element.record().length) {
            return element.record();
        }
        return Arrays.copyOfRange(element.record(), element.offset(), element.length());
    }
}
