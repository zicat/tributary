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

package org.zicat.tributary.sink.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.zicat.tributary.common.ConfigOption;
import org.zicat.tributary.common.ConfigOptions;
import org.zicat.tributary.common.ConfigOptions.StringSplitHandler;
import org.zicat.tributary.common.ReadableConfig;
import org.zicat.tributary.sink.function.Function;
import org.zicat.tributary.sink.function.FunctionFactory;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/** ElasticsearchFunctionFactory. */
public class ElasticsearchFunctionFactory implements FunctionFactory {

    public static final ConfigOption<List<String>> OPTION_HOSTS =
            ConfigOptions.key("hosts")
                    .listType(new StringSplitHandler(";"))
                    .description("Elasticsearch hosts to connect to, split by ;.")
                    .noDefaultValue();
    public static final ConfigOption<String> OPTION_PATH_PREFIX =
            ConfigOptions.key("path-prefix")
                    .stringType()
                    .description("Path prefix for Elasticsearch.")
                    .defaultValue(null);
    public static final ConfigOption<Boolean> OPTION_COMPRESSION =
            ConfigOptions.key("compression")
                    .booleanType()
                    .description("whether compression")
                    .defaultValue(false);
    public static final ConfigOption<String> OPTION_INDEX =
            ConfigOptions.key("index")
                    .stringType()
                    .description("Elasticsearch index for every record.")
                    .noDefaultValue();
    public static final ConfigOption<String> OPTION_PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .description("Password used to connect to Elasticsearch instance.")
                    .defaultValue(null);
    public static final ConfigOption<String> OPTION_USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .description("Username used to connect to Elasticsearch instance.")
                    .defaultValue(null);
    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("request.timeout")
                    .durationType()
                    .description(
                            "The timeout for requesting a connection from the connection manager.")
                    .defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("connection.timeout")
                    .durationType()
                    .description("The timeout for establishing a connection.")
                    .defaultValue(Duration.ofSeconds(10));
    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
            ConfigOptions.key("socket.timeout")
                    .durationType()
                    .description(
                            "The socket timeout (SO_TIMEOUT) for waiting for data or, put differently,a maximum period inactivity between two consecutive data packets.")
                    .defaultValue(Duration.ofSeconds(20));
    public static final ConfigOption<String> OPTION_REQUEST_INDEXER_IDENTITY =
            ConfigOptions.key("request.indexer.identity")
                    .stringType()
                    .description("The identity of request indexer to parse record.")
                    .defaultValue("default");
    public static final ConfigOption<Integer> OPTION_ASYNC_BULK_QUEUE_SIZE =
            ConfigOptions.key("async.bulk.queue.size")
                    .integerType()
                    .description(
                            "bulk async queue size, if task over this value will throw exception")
                    .defaultValue(1024);
    public static final ConfigOption<Duration> QUEUE_FULL_AWAIT_TIMEOUT =
            ConfigOptions.key("async.bulk.queue.await.timeout")
                    .durationType()
                    .description(
                            "await timeout when queue is full, default value is connection.request-timeout")
                    .defaultValue(Duration.ofSeconds(30));
    public static final ConfigOption<Integer> OPTION_BUCK_SIZE =
            ConfigOptions.key("bulk.size")
                    .integerType()
                    .description("bulk size")
                    .defaultValue(1000);
    public static final ConfigOption<Integer> OPTION_THREAD_MAX_PER_ROUTING =
            ConfigOptions.key("thread.max.per.routing")
                    .integerType()
                    .description("max thread per routing")
                    .defaultValue(5);

    public static final String IDENTITY = "elasticsearch";

    @Override
    public Function create() {
        return new ElasticsearchFunction();
    }

    @Override
    public String identity() {
        return IDENTITY;
    }

    /**
     * create rest client builder.
     *
     * @param config config
     * @return RestClientBuilder
     */
    public static RestClientBuilder createRestClientBuilder(ReadableConfig config) {
        final List<HttpHost> hosts =
                config.get(OPTION_HOSTS).stream()
                        .map(HttpHost::create)
                        .collect(Collectors.toList());
        final RestClientBuilder builder =
                RestClient.builder(hosts.toArray(new HttpHost[] {}))
                        .setCompressionEnabled(config.get(OPTION_COMPRESSION));
        final String pathPrefix = config.get(OPTION_PATH_PREFIX);
        if (pathPrefix != null && !pathPrefix.trim().isEmpty()) {
            builder.setPathPrefix(pathPrefix.trim());
        }

        final String username = config.get(OPTION_USERNAME);
        final String password = config.get(OPTION_PASSWORD);
        final int maxThreadPerRouting = config.get(OPTION_THREAD_MAX_PER_ROUTING);
        if (username != null
                && !username.trim().isEmpty()
                && password != null
                && !password.trim().isEmpty()) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(username.trim(), password.trim()));
            builder.setHttpClientConfigCallback(
                    (httpClientBuilder) ->
                            httpClientBuilder
                                    .setMaxConnPerRoute(maxThreadPerRouting)
                                    .setMaxConnTotal(maxThreadPerRouting * hosts.size())
                                    .setDefaultCredentialsProvider(credentialsProvider));
        }

        final long requestTimeoutMs = config.get(REQUEST_TIMEOUT).toMillis();
        final long connectionTimeoutMs = config.get(CONNECTION_TIMEOUT).toMillis();
        final long socketTimeoutMs = config.get(SOCKET_TIMEOUT).toMillis();
        builder.setRequestConfigCallback(
                (requestConfigBuilder) ->
                        requestConfigBuilder
                                .setConnectionRequestTimeout((int) requestTimeoutMs)
                                .setConnectTimeout((int) connectionTimeoutMs)
                                .setSocketTimeout((int) socketTimeoutMs));
        return builder;
    }
}
