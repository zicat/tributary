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

package org.zicat.tributary.sink.authentication;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import static org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytabAndReturnUGI;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** KerberosAuthenticatorFactory. */
public class KerberosAuthenticatorFactory {

    private static final PrivilegedExecutor SIMPLE_PRIVILEGED_EXECUTOR =
            new SimplePrivilegedExecutor();
    private static final Map<KerberosUser, PrivilegedExecutor> KERBEROS_USER_MAPPING =
            new ConcurrentHashMap<>();

    /**
     * execute with kerberos principal and keyTab.
     *
     * @param principal principal
     * @param keyTab keyTab
     * @param action action
     * @return T
     * @param <T> T
     * @throws Exception Exception
     */
    public static <T> T execute(
            String principal, String keyTab, PrivilegedExceptionAction<T> action) throws Exception {
        return getOrCreateKerberosExecutor(principal, keyTab).execute(action);
    }

    /**
     * get or create KerberosExecutor.
     *
     * @param principal principal
     * @param keyTab keyTab
     * @return PrivilegedExecutor
     */
    public static PrivilegedExecutor getOrCreateKerberosExecutor(String principal, String keyTab)
            throws IOException {
        if (Strings.isNullOrEmpty(principal) && Strings.isNullOrEmpty(keyTab)) {
            return SIMPLE_PRIVILEGED_EXECUTOR;
        }
        final KerberosUser kerberosUser = new KerberosUser(principal, keyTab);
        final PrivilegedExecutor privilegedExecutor = KERBEROS_USER_MAPPING.get(kerberosUser);
        if (privilegedExecutor != null) {
            return privilegedExecutor;
        }
        synchronized (KERBEROS_USER_MAPPING) {
            final PrivilegedExecutor existingExecutor = KERBEROS_USER_MAPPING.get(kerberosUser);
            if (existingExecutor != null) {
                return existingExecutor;
            }
            final PrivilegedExecutor newExecutor = createPrivilegedExecutor(kerberosUser);
            KERBEROS_USER_MAPPING.put(kerberosUser, newExecutor);
            return newExecutor;
        }
    }

    /**
     * createPrivilegedExecutor.
     *
     * @param kerberosUser kerberosUser
     * @return PrivilegedExecutor
     * @throws IOException IOException
     */
    private static PrivilegedExecutor createPrivilegedExecutor(KerberosUser kerberosUser)
            throws IOException {
        final String principal = kerberosUser.getPrincipal();
        final String keytab = kerberosUser.getKeyTab();
        File keytabFile = new File(keytab);
        Preconditions.checkArgument(
                keytabFile.isFile() && keytabFile.canRead(),
                "Keytab is not a readable file: " + keytab);
        // resolve the requested principal
        String resolvedPrincipal;
        try {
            resolvedPrincipal = SecurityUtil.getServerPrincipal(principal, "");
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Host lookup error resolving kerberos principal " + principal, e);
        }
        if (!UserGroupInformation.isSecurityEnabled()) {
            Configuration conf = new Configuration(false);
            conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
            UserGroupInformation.setConfiguration(conf);
        }
        return new UGIPrivilegedExecutor(
                loginUserFromKeytabAndReturnUGI(resolvedPrincipal, keytab));
    }
}
