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

import java.util.Objects;

/** KerberosUser. */
public class KerberosUser {

    private final String principal;
    private final String keyTab;

    public KerberosUser(String principal, String keyTab) {
        this.principal = principal;
        this.keyTab = keyTab;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getKeyTab() {
        return keyTab;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final KerberosUser other = (KerberosUser) obj;
        if (!Objects.equals(this.principal, other.principal)) {
            return false;
        }
        return Objects.equals(this.keyTab, other.keyTab);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + (this.principal != null ? this.principal.hashCode() : 0);
        hash = 41 * hash + (this.keyTab != null ? this.keyTab.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "{ principal: " + principal + ", keytab: " + keyTab + " }";
    }
}
