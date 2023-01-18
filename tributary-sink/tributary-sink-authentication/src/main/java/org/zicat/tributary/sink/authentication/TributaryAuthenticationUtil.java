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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/** TributaryAuthenticationUtil. */
public class TributaryAuthenticationUtil {

    private TributaryAuthenticationUtil() {}

    private static KerberosAuthenticator kerbAuthenticator;

    /**
     * If principal and keytab are null, this method returns a SimpleAuthenticator which executes
     * without authentication. If valid credentials are provided KerberosAuthenticator is returned
     * which can be used to execute as the authenticated principal. Invalid credentials result in
     * IllegalArgumentException and Failure to authenticate results in SecurityException.
     *
     * @param principal principal
     * @param keytab keytab
     * @return TributaryAuthenticator
     */
    public static synchronized TributaryAuthenticator getAuthenticator(
            String principal, String keytab) throws SecurityException {

        if (Strings.isNullOrEmpty(principal) && Strings.isNullOrEmpty(keytab)) {
            return SimpleAuthenticator.getSimpleAuthenticator();
        }

        Preconditions.checkArgument(
                principal != null, "Principal can not be null when keytab is provided");
        Preconditions.checkArgument(
                keytab != null, "Keytab can not be null when Principal is provided");

        if (kerbAuthenticator == null) {
            kerbAuthenticator = new KerberosAuthenticator();
        }
        kerbAuthenticator.authenticate(principal, keytab);

        return kerbAuthenticator;
    }

    @VisibleForTesting
    public static void clearCredentials() {
        kerbAuthenticator = null;
    }
}
