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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

/** host utils. */
public class HostUtils {

    private static final String KEY_HOSTNAME = "HOSTNAME";
    private static final String KEY_COMPUTERNAME = "COMPUTERNAME";
    private static final String ALL_IP_FILTER_PATTERN = ".*";

    /**
     * get localhost string ip by pattern filter.
     *
     * @param pattern pattern
     * @return string host
     */
    public static String geHostAddress(String pattern) {
        return getInetAddress(pattern).getHostAddress();
    }

    /**
     * get localhost string ip list by pattern filter.
     *
     * @param patterns patterns
     * @return list hosts
     */
    public static List<String> getHostAddresses(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            return Collections.emptyList();
        }
        return patterns.stream().map(HostUtils::geHostAddress).collect(Collectors.toList());
    }

    /**
     * get localhost string ip by pattern filter.
     *
     * @return host
     */
    private static InetAddress getInetAddress(String pattern) {
        if (pattern == null) {
            pattern = ALL_IP_FILTER_PATTERN;
        }
        if (pattern.equals("localhost") || pattern.equals("127.0.0.1")) {
            return InetAddress.getLoopbackAddress();
        }
        final Enumeration<NetworkInterface> it;
        try {
            it = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new TributaryRuntimeException(e);
        }
        while (it.hasMoreElements()) {
            final NetworkInterface networkInterface = it.nextElement();
            final Enumeration<InetAddress> addressIt = networkInterface.getInetAddresses();
            while (addressIt.hasMoreElements()) {
                final InetAddress inetAddress = addressIt.nextElement();
                if (!inetAddress.isLoopbackAddress()
                        && inetAddress instanceof Inet4Address
                        && inetAddress.getHostAddress().matches(pattern)) {
                    return inetAddress;
                }
            }
        }
        throw new TributaryRuntimeException("inet address not found by ip pattern " + pattern);
    }

    /**
     * get host name.
     *
     * @return hostname
     */
    public static String getHostName() {
        String hostname = System.getenv(KEY_HOSTNAME);
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }

        hostname = System.getenv(KEY_COMPUTERNAME);
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
