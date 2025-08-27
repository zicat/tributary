/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.tributary.source.logstash.base.utils;

import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zicat.tributary.common.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utilities to compile a generated code to a Class. */
public final class CompileUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CompileUtils.class);
    private static final String PATTERN_PACKAGE = "package\\s+[^;]+;";
    private static final Pattern PATTERN_CLAS_NAME =
            Pattern.compile("class\\s+(\\w+)\\s");

    @SuppressWarnings("unchecked")
    public static synchronized <T> Class<T> doCompile(String name, String code) {
        code = code.replaceAll(PATTERN_PACKAGE, "");
        if (Strings.isBlank(name)) {
            Matcher matcher = PATTERN_CLAS_NAME.matcher(code);
            if (matcher.find()) {
                name = matcher.group(1);
            } else {
                throw new RuntimeException("Can not find class name");
            }
        }
        LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
        final SimpleCompiler compiler = new SimpleCompiler();
        try {
            compiler.cook(code);
        } catch (Throwable t) {
            throw new RuntimeException(addLineNumber(code), t);
        }
        try {
            return (Class<T>) compiler.getClassLoader().loadClass(name);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Can not load class " + name, e);
        }
    }

    /**
     * Compile the code to a Class. The class name must be defined in the code.
     *
     * @param code code
     * @return class
     * @param <T> T
     */
    public static synchronized <T> Class<T> doCompile(String code) {
        return doCompile(null, code);
    }

    /**
     * To output more information when an error occurs. Generally, when cook fails, it shows which
     * line is wrong. This line number starts at 1.
     */
    private static String addLineNumber(String code) {
        String[] lines = code.split("\n");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
        }
        return builder.toString();
    }
}
