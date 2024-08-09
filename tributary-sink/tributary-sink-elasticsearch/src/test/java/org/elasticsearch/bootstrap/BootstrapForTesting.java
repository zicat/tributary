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

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.network.IfConfig;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.jdk.JarHell;
import org.zicat.tributary.sink.elasticsearch.test.utils.OSUtils;
import org.zicat.tributary.sink.elasticsearch.test.utils.OSUtils.OSType;

import java.net.URL;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** AgoraBootstrapForTesting. */
public class BootstrapForTesting {

    // TODO: can we share more code with the non-test side here
    // without making things complex???

    static {
        // make sure java.io.tmpdir exists always (in case code uses it in a static initializer)
        Path javaTmpDir =
                PathUtils.get(
                        Objects.requireNonNull(
                                System.getProperty("java.io.tmpdir"),
                                "please set ${java.io.tmpdir} in pom.xml"));
        try {
            Security.ensureDirectoryExists(javaTmpDir);
        } catch (Exception e) {
            throw new RuntimeException("unable to create test temp directory", e);
        }

        // just like bootstrap, initialize natives, then SM
        //        Bootstrap.initializeNatives(javaTmpDir, true, true, true);
        if (OSUtils.osType() != OSType.LINUX) {
            Bootstrap.initializeNatives(javaTmpDir, true, true, true);
        }

        // initialize probes
        Bootstrap.initializeProbes();

        // initialize sysprops
        BootstrapInfo.getSystemProperties();

        // Log ifconfig output before SecurityManager is installed
        IfConfig.logIfNecessary();
    }

    /** Add the codebase url of the given classname to the codebases map, if the class exists. */
    private static void addClassCodebase(
            Map<String, URL> codebases, String name, String classname) {
        try {
            Class clazz = BootstrapForTesting.class.getClassLoader().loadClass(classname);
            if (codebases.put(name, clazz.getProtectionDomain().getCodeSource().getLocation())
                    != null) {
                throw new IllegalStateException("Already added " + name + " codebase for testing");
            }
        } catch (ClassNotFoundException e) {
            // no class, fall through to not add. this can happen for any tests that do not include
            // the given class. eg only core tests include plugin-classloader
        }
    }

    /**
     * return parsed classpath, but with symlinks resolved to destination files for matching this is
     * for matching the toRealPath() in the code where we have a proper plugin structure.
     */
    static Set<URL> parseClassPathWithSymlinks() throws Exception {
        Set<URL> raw = JarHell.parseClassPath();
        Set<URL> cooked = new HashSet<>(raw.size());
        for (URL url : raw) {
            boolean added = cooked.add(PathUtils.get(url.toURI()).toRealPath().toUri().toURL());
            if (!added) {
                throw new IllegalStateException(
                        "Duplicate in classpath after resolving symlinks: " + url);
            }
        }
        return raw;
    }

    // does nothing, just easy way to make sure the class is loaded.
    public static void ensureInitialized() {}
}
