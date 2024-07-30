/*
 * Copyright 2021-2023 Aklivity Inc
 *
 * Licensed under the Aklivity Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 *   https://www.aklivity.io/aklivity-community-license/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.aklivity.zilla.runtime.binding.asyncapi.internal.config.composite;

import static io.aklivity.zilla.runtime.engine.config.KindConfig.CLIENT;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.aklivity.zilla.runtime.binding.asyncapi.config.AsyncapiSchemaConfig;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.config.AsyncapiBindingConfig;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.config.AsyncapiCompositeConditionConfig;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.config.AsyncapiCompositeConfig;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.config.AsyncapiCompositeRouteConfig;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.view.AsyncapiChannelView;
import io.aklivity.zilla.runtime.binding.asyncapi.internal.view.AsyncapiMessageView;
import io.aklivity.zilla.runtime.binding.kafka.config.KafkaOptionsConfig;
import io.aklivity.zilla.runtime.binding.kafka.config.KafkaOptionsConfigBuilder;
import io.aklivity.zilla.runtime.binding.kafka.config.KafkaSaslConfig;
import io.aklivity.zilla.runtime.binding.kafka.config.KafkaTopicConfig;
import io.aklivity.zilla.runtime.binding.kafka.config.KafkaTopicConfigBuilder;
import io.aklivity.zilla.runtime.engine.config.KindConfig;
import io.aklivity.zilla.runtime.engine.config.NamespaceConfig;
import io.aklivity.zilla.runtime.engine.config.NamespaceConfigBuilder;
import io.aklivity.zilla.runtime.model.avro.config.AvroModelConfig;

public final class AsyncapiClientGenerator extends AsyncapiCompositeGenerator
{
    @Override
    protected AsyncapiCompositeConfig generate(
        AsyncapiBindingConfig binding,
        List<AsyncapiSchemaConfig> schemas)
    {
        List<NamespaceConfig> namespaces = new LinkedList<>();
        List<AsyncapiCompositeRouteConfig> routes = new LinkedList<>();
        for (AsyncapiSchemaConfig schema : schemas)
        {
            NamespaceHelper helper = new ClientNamespaceHelper(binding, schema);
            NamespaceConfig namespace = NamespaceConfig.builder()
                    .inject(helper::injectAll)
                    .build();

            namespaces.add(namespace);

            Matcher routed = Pattern.compile("(http|sse|mqtt|kafka)(?:_cache)?_client0").matcher("");
            namespace.bindings.stream()
                .filter(b -> routed.reset(b.type).matches())
                .forEach(b ->
                {
                    final String operationType = routed.group(1);
                    final int operationTypeId = binding.supplyTypeId.applyAsInt(operationType);

                    routes.add(new AsyncapiCompositeRouteConfig(
                        b.resolveId.applyAsLong(b.name),
                        new AsyncapiCompositeConditionConfig(
                            schema.schemaId,
                            operationTypeId)));
                });
        }

        return new AsyncapiCompositeConfig(schemas, namespaces, routes);
    }

    private final class ClientNamespaceHelper extends NamespaceHelper
    {
        private final CatalogsHelper catalogs;
        private final BindingsHelper bindings;

        private ClientNamespaceHelper(
            AsyncapiBindingConfig config,
            AsyncapiSchemaConfig schema)
        {
            super(config, schema);
            this.catalogs = new CatalogsHelper();
            this.bindings = new ClientBindingsHelper();
        }

        protected <C> NamespaceConfigBuilder<C> injectComponents(
            NamespaceConfigBuilder<C> namespace)
        {
            return namespace
                    .inject(catalogs::injectAll)
                    .inject(bindings::injectAll);
        }

        private final class ClientBindingsHelper extends BindingsHelper
        {
            private static final Pattern PARAMETERIZED_TOPIC_PATTERN = Pattern.compile(REGEX_ADDRESS_PARAMETER);

            private final Map<String, NamespaceInjector> protocols;
            private final List<String> secure;

            private ClientBindingsHelper()
            {
                this.protocols = Map.of(
                    "kafka", this::injectKafka,
                    "kafka-secure", this::injectKafkaSecure,
                    "http", this::injectHttp,
                    "https", this::injectHttps,
                    "mqtt", this::injectMqtt,
                    "mqtts", this::injectMqtts);
                this.secure = List.of("kafka-secure", "https", "mqtts");
            }

            @Override
            protected <C> NamespaceConfigBuilder<C> injectAll(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                        .inject(this::injectProtocols)
                        .inject(this::injectTlsClient)
                        .inject(this::injectTcpClient);
            }

            private <C> NamespaceConfigBuilder<C> injectProtocols(
                NamespaceConfigBuilder<C> namespace)
            {
                Stream.of(schema)
                    .map(s -> s.asyncapi)
                    .flatMap(v -> v.servers.stream())
                    .map(s -> s.protocol)
                    .distinct()
                    .map(protocols::get)
                    .forEach(p -> p.inject(namespace));

                return namespace;
            }

            private <C> NamespaceConfigBuilder<C> injectTlsClient(
                NamespaceConfigBuilder<C> namespace)
            {
                if (Stream.of(schema)
                        .map(s -> s.asyncapi)
                        .flatMap(v -> v.servers.stream())
                        .filter(s -> secure.contains(s.protocol))
                        .count() != 0L)
                {
                    namespace
                        .binding()
                            .name("tls_client0")
                            .type("tls")
                            .kind(CLIENT)
                            .inject(this::injectMetrics)
                            .options(config.options.tls)
                            .vault(config.qvault)
                            .exit("tcp_client0")
                            .build();
                }

                return namespace;
            }

            private <C> NamespaceConfigBuilder<C> injectTcpClient(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                    .name("tcp_client0")
                    .type("tcp")
                    .kind(CLIENT)
                    .inject(this::injectMetrics)
                    .options(config.options.tcp)
                .build();

            }

            private <C> NamespaceConfigBuilder<C> injectKafka(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                        .name("kafka_client0")
                        .type("kafka")
                        .kind(CLIENT)
                        .options(KafkaOptionsConfig::builder)
                            .inject(this::injectKafkaSaslOptions)
                            .inject(this::injectKafkaServerOptions)
                            .build()
                        .inject(this::injectMetrics)
                        .exit("tcp_client0")
                        .build();
            }

            private <C> NamespaceConfigBuilder<C> injectKafkaSecure(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .inject(this::injectKafkaCache)
                    .binding()
                        .name("kafka_client0")
                        .type("kafka")
                        .kind(CLIENT)
                        .options(KafkaOptionsConfig::builder)
                            .inject(this::injectKafkaSaslOptions)
                            .inject(this::injectKafkaServerOptions)
                            .build()
                        .inject(this::injectMetrics)
                        .exit("tls_client0")
                        .build();
            }

            private <C> NamespaceConfigBuilder<C> injectKafkaCache(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                        .binding()
                            .name("kafka_cache_client0")
                            .type("kafka")
                            .kind(KindConfig.CACHE_CLIENT)
                            .inject(this::injectMetrics)
                            .options(KafkaOptionsConfig::builder)
                                .inject(this::injectKafkaTopicOptions)
                                .build()
                            .exit("kafka_cache_server0")
                        .build()
                        .binding()
                            .name("kafka_cache_server0")
                            .type("kafka")
                            .kind(KindConfig.CACHE_SERVER)
                            .inject(this::injectMetrics)
                            .options(KafkaOptionsConfig::builder)
                                .inject(this::injectKafkaBootstrapOptions)
                                .inject(this::injectKafkaTopicOptions)
                                .build()
                            .exit("kafka_client0")
                        .build();
            }

            private <C> KafkaOptionsConfigBuilder<C> injectKafkaTopicOptions(
                KafkaOptionsConfigBuilder<C> options)
            {
                List<KafkaTopicConfig> topics = config.options.kafka != null
                        ? config.options.kafka.topics
                        : null;

                if (topics != null)
                {
                    Stream.of(schema)
                        .map(s -> s.asyncapi)
                        .flatMap(v -> v.operations.values().stream())
                        .filter(o -> o.channel.hasMessages() || o.channel.hasParameters())
                        .flatMap(o -> Stream.of(o.channel, o.reply != null ? o.reply.channel : null))
                        .filter(c -> c != null)
                        .forEach(channel ->
                            topics.stream()
                                .filter(t -> t.name.equals(channel.address))
                                .findFirst()
                                .ifPresent(topic ->
                                    options
                                        .topic()
                                            .name(channel.address)
                                            .headers(topic.headers)
                                            .inject(t -> injectKafkaTopicKey(t, channel))
                                            .inject(t -> injectKafkaTopicValue(t, channel))
                                            .build()
                                        .build()));
                }

                return options;
            }

            private <C> KafkaTopicConfigBuilder<C> injectKafkaTopicKey(
                KafkaTopicConfigBuilder<C> topic,
                AsyncapiChannelView channel)
            {
                if (channel.hasMessages())
                {
                    channel.messages.stream()
                        .filter(m -> m.bindings != null && m.bindings.kafka != null && m.bindings.kafka.key != null)
                        .forEach(message ->
                            topic.key(AvroModelConfig::builder) // TODO: assumes AVRO
                                .catalog()
                                    .name("catalog0")
                                    .schema()
                                        .version("latest")
                                        .subject(message.name)
                                        .build()
                                    .build());
                }
                return topic;
            }

            private <C> KafkaTopicConfigBuilder<C> injectKafkaTopicValue(
                KafkaTopicConfigBuilder<C> topic,
                AsyncapiChannelView channel)
            {
                if (channel.hasMessages())
                {
                    final AsyncapiMessageView message = channel.messages.get(0);

                    injectPayloadModel(topic::value, message);
                }

                return topic;
            }

            private <C> KafkaOptionsConfigBuilder<C> injectKafkaBootstrapOptions(
                KafkaOptionsConfigBuilder<C> options)
            {
                Stream.of(schema)
                    .map(s -> s.asyncapi)
                    .flatMap(v -> v.channels.stream())
                    .filter(c -> !PARAMETERIZED_TOPIC_PATTERN.matcher(c.address).find())
                    .map(c -> c.address)
                    .distinct()
                    .forEach(options::bootstrap);

                return options;
            }

            private <C> KafkaOptionsConfigBuilder<C> injectKafkaSaslOptions(
                KafkaOptionsConfigBuilder<C> options)
            {
                KafkaSaslConfig sasl = config.options != null && config.options.kafka != null
                    ? config.options.kafka.sasl
                    : null;

                if (sasl != null)
                {
                    options.sasl()
                        .mechanism(sasl.mechanism)
                        .username(sasl.username)
                        .password(sasl.password)
                        .build();
                }

                return options;
            }

            private <C> KafkaOptionsConfigBuilder<C> injectKafkaServerOptions(
                KafkaOptionsConfigBuilder<C> options)
            {
                config.options.specs.stream()
                    .flatMap(s -> s.servers.stream())
                    .map(s -> s.url != null ? s.url : s.host)
                    .distinct()
                    .map(h -> h.split(":"))
                    .forEach(hp ->
                    {
                        options.server()
                            .host(hp[0])
                            .port(Integer.parseInt(hp[1]))
                            .build();
                    });

                return options;
            }

            private <C> NamespaceConfigBuilder<C> injectHttp(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                        .name("http_client0")
                        .type("http")
                        .kind(CLIENT)
                        .inject(this::injectMetrics)
                        .exit("tcp_client0")
                        .build();
            }

            private <C> NamespaceConfigBuilder<C> injectHttps(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                        .name("http_client0")
                        .type("http")
                        .kind(CLIENT)
                        .inject(this::injectMetrics)
                        .exit("tls_client0")
                        .build();
            }

            private <C> NamespaceConfigBuilder<C> injectMqtt(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                        .name("mqtt_client0")
                        .type("mqtt")
                        .kind(CLIENT)
                        .inject(this::injectMetrics)
                        .exit("tcp_client0")
                        .build();
            }

            private <C> NamespaceConfigBuilder<C> injectMqtts(
                NamespaceConfigBuilder<C> namespace)
            {
                return namespace
                    .binding()
                        .name("mqtt_client0")
                        .type("mqtt")
                        .kind(CLIENT)
                        .inject(this::injectMetrics)
                        .exit("tls_client0")
                        .build();
            }
        }
    }
}
