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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;

import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/**
 * Config options that is used to configure a Pulsar SQL Connector. These config options are
 * specific to SQL Connectors only. Other runtime configurations can be found in {@link
 * org.apache.flink.connector.pulsar.common.config.PulsarOptions}, {@link
 * org.apache.flink.connector.pulsar.source.PulsarSourceOptions}, and {@link
 * org.apache.flink.connector.pulsar.sink.PulsarSinkOptions}.
 */
public class PulsarTableOptions {

    private PulsarTableOptions() {}

    public static final ConfigOption<List<String>> TOPICS =
            ConfigOptions.key("topics")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. It is required for both source and sink");

    // --------------------------------------------------------------------------------------------
    // Table Source Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<SubscriptionType> SOURCE_SUBSCRIPTION_TYPE =
            ConfigOptions.key("source.subscription-type")
                    .enumType(SubscriptionType.class)
                    .defaultValue(SubscriptionType.Exclusive)
                    .withDescription(
                            "Subscription type for Pulsar source to use. Only \"Exclusive\" and \"Shared\" are allowed.");

    /**
     * Exactly same as {@link
     * org.apache.flink.connector.pulsar.source.PulsarSourceOptions#PULSAR_SUBSCRIPTION_NAME}.
     * Copied because we want to have a default value for it.
     */
    public static final ConfigOption<String> SOURCE_SUBSCRIPTION_NAME =
            ConfigOptions.key("source.subscription-name")
                    .stringType()
                    .defaultValue("flink-sql-connector-pulsar")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specify the subscription name consumer used by runtime PulsarSource.")
                                    .text(
                                            " This argument is required when constructing the consumer.")
                                    .build());

    public static final ConfigOption<String> SOURCE_START_FROM_MESSAGE_ID =
            ConfigOptions.key("source.start.message-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional message id used to specify a consuming starting point for "
                                    + "source. Use \"earliest\", \"latest\" or pass in a message id "
                                    + "representation in \"ledgerId:entryId:partitionId\", "
                                    + "such as \"12:2:-1\"");

    public static final ConfigOption<Long> SOURCE_START_FROM_PUBLISH_TIME =
            ConfigOptions.key("source.start.publish-time")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional publish timestamp used to specify a consuming starting point for source.");

    // --------------------------------------------------------------------------------------------
    // Table Sink Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> SINK_CUSTOM_TOPIC_ROUTER =
            ConfigOptions.key("sink.custom-topic-router")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional custom TopicRouter implementation class URL to use in sink. If this option"
                                    + "is provided, \"sink.topic-routing-mode\" will be ignored.");

    public static final ConfigOption<TopicRoutingMode> SINK_TOPIC_ROUTING_MODE =
            ConfigOptions.key("sink.topic-routing-mode")
                    .enumType(TopicRoutingMode.class)
                    .defaultValue(TopicRoutingMode.ROUND_ROBIN)
                    .withDescription(
                            "Optional TopicRoutingMode. There are \"round-robin\" and "
                                    + "\"message-key-hash\" two options. Default use"
                                    + "\"round-robin\", if you want to use a custom"
                                    + "TopicRouter implementation, use \"sink.custom-topic-router\"");

    public static final ConfigOption<Duration> SINK_MESSAGE_DELAY_INTERVAL =
            ConfigOptions.key("sink.message-delay-interval")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription("Optional sink message delay delivery interval.");

    // --------------------------------------------------------------------------------------------
    // Format Options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for decoding/encoding key bytes in "
                                    + "Pulsar message. The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the "
                                    + "table schema which should be decoded/encoded "
                                    + "from the key bytes of a Pulsar message. By default, "
                                    + "this list is empty and thus a key is undefined.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for decoding/encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

    // --------------------------------------------------------------------------------------------
    // Pulsar Options
    // --------------------------------------------------------------------------------------------

    /**
     * Exactly same as {@link
     * org.apache.flink.connector.pulsar.common.config.PulsarOptions#PULSAR_ADMIN_URL}. Copied here
     * because it is a required config option and should not be included in the {@link
     * org.apache.flink.table.factories.FactoryUtil.FactoryHelper#validateExcept(String...)} method.
     *
     * <p>By default all {@link org.apache.flink.connector.pulsar.common.config.PulsarOptions} are
     * included in the validateExcept() method./p>
     */
    public static final ConfigOption<String> ADMIN_URL =
            ConfigOptions.key("admin-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The Pulsar service HTTP URL for the admin endpoint. For example, %s, or %s for TLS.",
                                            code("http://my-broker.example.com:8080"),
                                            code("https://my-broker.example.com:8443"))
                                    .build());

    /**
     * Exactly same as {@link
     * org.apache.flink.connector.pulsar.common.config.PulsarOptions#PULSAR_SERVICE_URL}. Copied
     * here because it is a required config option and should not be included in the {@link
     * org.apache.flink.table.factories.FactoryUtil.FactoryHelper#validateExcept(String...)} method.
     *
     * <p>By default all {@link org.apache.flink.connector.pulsar.common.config.PulsarOptions} are
     * included in the validateExcept() method./p>
     */
    public static final ConfigOption<String> SERVICE_URL =
            ConfigOptions.key("service-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Service URL provider for Pulsar service.")
                                    .linebreak()
                                    .text(
                                            "To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.")
                                    .linebreak()
                                    .text(
                                            "You can assign Pulsar protocol URLs to specific clusters and use the Pulsar scheme.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "This is an example of %s: %s.",
                                                    code("localhost"),
                                                    code("pulsar://localhost:6650")),
                                            text(
                                                    "If you have multiple brokers, the URL is as: %s",
                                                    code(
                                                            "pulsar://localhost:6550,localhost:6651,localhost:6652")),
                                            text(
                                                    "A URL for a production Pulsar cluster is as: %s",
                                                    code(
                                                            "pulsar://pulsar.us-west.example.com:6650")),
                                            text(
                                                    "If you use TLS authentication, the URL is as %s",
                                                    code(
                                                            "pulsar+ssl://pulsar.us-west.example.com:6651")))
                                    .build());

    public static final ConfigOption<Boolean> EXPLICIT =
            ConfigOptions.key("explicit")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Indicate if the table is an explict flink table");
}
