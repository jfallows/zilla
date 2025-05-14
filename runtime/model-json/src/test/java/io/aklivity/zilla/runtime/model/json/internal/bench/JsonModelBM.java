/*
 * Copyright 2021-2024 Aklivity Inc
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
package io.aklivity.zilla.runtime.model.json.internal.bench;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.aklivity.zilla.runtime.engine.Configuration;
import io.aklivity.zilla.runtime.engine.EngineContext;
import io.aklivity.zilla.runtime.engine.config.CatalogConfig;
import io.aklivity.zilla.runtime.engine.config.CatalogedConfig;
import io.aklivity.zilla.runtime.engine.model.ConverterHandler;
import io.aklivity.zilla.runtime.engine.model.ValidatorHandler;
import io.aklivity.zilla.runtime.engine.model.function.ValueConsumer;
import io.aklivity.zilla.runtime.engine.test.internal.catalog.TestCatalog;
import io.aklivity.zilla.runtime.engine.test.internal.catalog.config.TestCatalogOptionsConfig;
import io.aklivity.zilla.runtime.model.json.config.JsonModelConfig;
import io.aklivity.zilla.runtime.model.json.internal.JsonModel;
import io.aklivity.zilla.runtime.model.json.internal.JsonModelContext;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class JsonModelBM
{
    @State(Scope.Benchmark)
    public static class BenchmarkState
    {
        final String schema = """
                {
                    "type": "object",
                    "properties":
                    {
                        "string":
                        {
                            "type": "string"
                        },
                        "number":
                        {
                            "type": "number"
                        },
                        "boolean":
                        {
                            "type": "boolean"
                        }
                    }
                }
                """;

        private long catalogId = Long.MAX_VALUE;

        private TestCatalog catalog = new TestCatalog(new Configuration());

        private JsonModel model =  new JsonModel();

        JsonModelConfig config;

        @Setup
        public void setup()
        {
            CatalogedConfig cataloged = CatalogedConfig.builder()
                .name("test")
                .schema()
                    .id(1234)
                    .build()
                .build();
            cataloged.id = catalogId;

            config = JsonModelConfig.builder()
                .catalog(cataloged)
                .build();
        }

        JsonModelContext supplyModel()
        {
            EngineContext engine = mock(EngineContext.class);

            when(engine.supplyCatalog(catalogId))
                .thenReturn(
                    catalog.supply(engine).attach(
                        CatalogConfig.builder()
                            .namespace("benchmark")
                            .name("test0")
                            .type("test")
                            .options(TestCatalogOptionsConfig::builder)
                            .id(1234)
                            .schema(schema)
                            .build()
                        .build()));

            return model.supply(engine);
        }
    }

    @State(Scope.Thread)
    public static class ModelState
    {
        final String document = """
            {
                "string": "text",
                "number": 123,
                "boolean": true
            }
            """;

        final DirectBuffer input = new UnsafeBuffer(document.getBytes(UTF_8));
        final MutableDirectBuffer output = new UnsafeBuffer(new byte[input.capacity()]);
        final ValueConsumer consumer = (b, o, l) -> output.putBytes(0, b, o, l);

    }

    @State(Scope.Thread)
    public static class ValidatorState extends ModelState
    {
        ValidatorHandler validator;

        @Setup
        public void setup(
            BenchmarkState state)
        {
            JsonModelContext context = state.supplyModel();

            validator = context.supplyValidatorHandler(state.config);
        }
    }

    @State(Scope.Thread)
    public static class ConverterState extends ModelState
    {
        ConverterHandler converter;

        @Setup
        public void setup(
            BenchmarkState state)
        {
            JsonModelContext context = state.supplyModel();

            converter = context.supplyReadConverterHandler(state.config);
        }
    }

    @Benchmark
    public void validate(
        ValidatorState state) throws Exception
    {
        DirectBuffer data = state.input;
        ValueConsumer consumer = state.consumer;
        state.validator.validate(0L, 0L, data, 0, data.capacity(), consumer);
    }

    @Benchmark
    public void convert(
        ConverterState state) throws Exception
    {
        DirectBuffer data = state.input;
        ValueConsumer consumer = state.consumer;
        state.converter.convert(0L, 0L, data, 0, data.capacity(), consumer);
    }

    public static void main(
        String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(JsonModelBM.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
