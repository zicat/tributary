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

package org.zicat.tributary.channel.file;

/** FileSegmentUtil. */
public class FileSegmentUtil {

    public static final String FILE_DEFAULT_PREFIX = "segment_";
    public static final String FILE_DEFAULT_SUFFIX = ".log";
    public static final int FILE_SEGMENT_HEAD_SIZE = 8;

    /**
     * legal offset, return SEGMENT_HEAD_SIZE if offset < 8.
     *
     * @param offset offset
     * @return offset
     */
    public static long legalFileOffset(long offset) {
        return offset < FILE_SEGMENT_HEAD_SIZE ? FILE_SEGMENT_HEAD_SIZE : offset;
    }

    /**
     * get fileId by fileName.
     *
     * @param fileName fileName
     * @return fileId
     */
    public static long getIdByName(String filePrefix, String fileName) {
        return Long.parseLong(
                fileName.substring(
                        realPrefix(filePrefix, FILE_DEFAULT_PREFIX).length(),
                        fileName.length() - FILE_DEFAULT_SUFFIX.length()));
    }

    /**
     * get fileName by fileId.
     *
     * @param fileId fileId
     * @return fileName
     */
    public static String getNameById(String filePrefix, long fileId) {
        return realPrefix(filePrefix, FILE_DEFAULT_PREFIX) + fileId + FILE_DEFAULT_SUFFIX;
    }

    /**
     * get real prefix.
     *
     * @param filePrefix filePrefix
     * @param defaultValue defaultValue
     * @return prefix name
     */
    public static String realPrefix(String filePrefix, String defaultValue) {
        return filePrefix == null ? defaultValue : filePrefix + "_" + defaultValue;
    }

    /**
     * check file is segment file.
     *
     * @param fileName fileName
     * @return true if log segment else false
     */
    public static boolean isFileSegment(String filePrefix, String fileName) {
        if (!fileName.startsWith(realPrefix(filePrefix, FILE_DEFAULT_PREFIX))
                || !fileName.endsWith(FILE_DEFAULT_SUFFIX)) {
            return false;
        }
        try {
            return getIdByName(filePrefix, fileName) >= 0;
        } catch (Exception e) {
            return false;
        }
    }
}
