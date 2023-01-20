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

package org.zicat.tributary.service;

import io.prometheus.client.exporter.MetricsServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.atomic.AtomicBoolean;

/** TributaryServiceApplication. */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
public class TributaryServiceApplication extends SpringBootServletInitializer {

    private static ConfigurableApplicationContext applicationContext;
    private static final Logger LOG = LoggerFactory.getLogger(TributaryServiceApplication.class);
    private static final AtomicBoolean closed = new AtomicBoolean(false);

    public static void main(String[] args) {
        applicationContext = SpringApplication.run(TributaryServiceApplication.class, args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LOG.info("Stop Tributary Service")));
    }

    @Bean
    public ServletRegistrationBean<MetricsServlet> getServletRegistrationBean() {
        final ServletRegistrationBean<MetricsServlet> bean =
                new ServletRegistrationBean<>(new MetricsServlet());
        bean.addUrlMappings("/metrics");
        return bean;
    }

    /** shutdown dispatcher spring application. */
    public static void shutdown() {
        if (closed.compareAndSet(false, true)) {
            SpringApplication.exit(applicationContext);
        }
    }
}
