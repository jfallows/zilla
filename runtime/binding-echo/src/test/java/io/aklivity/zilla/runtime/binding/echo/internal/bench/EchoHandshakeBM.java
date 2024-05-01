/*
 * Copyright 2021-2023 Aklivity Inc.
 *
 * Aklivity licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.aklivity.zilla.runtime.binding.echo.internal.bench;

import static io.aklivity.zilla.runtime.engine.config.KindConfig.SERVER;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import io.aklivity.zilla.runtime.binding.echo.internal.types.stream.BeginFW;
import io.aklivity.zilla.runtime.binding.echo.internal.types.stream.BeginFields;
import io.aklivity.zilla.runtime.binding.echo.internal.types.stream.WindowFW;
import io.aklivity.zilla.runtime.binding.echo.internal.types.stream.WindowFields;
import io.aklivity.zilla.runtime.engine.Configuration;
import io.aklivity.zilla.runtime.engine.binding.BindingContext;
import io.aklivity.zilla.runtime.engine.binding.BindingFactory;
import io.aklivity.zilla.runtime.engine.binding.BindingHandler;
import io.aklivity.zilla.runtime.engine.binding.function.MessageConsumer;
import io.aklivity.zilla.runtime.engine.config.BindingConfig;
import io.aklivity.zilla.runtime.engine.config.NamespaceConfig;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 10, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class EchoHandshakeBM
{
    private static final int BUFFER_SIZE = 1024 * 8;

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private MemorySegment writeSegment;
    private MutableDirectBuffer writeBuffer;

    private BindingHandler handler;
    private Runnable detacher;

    @Setup(Level.Trial)
    public void init() throws IOException
    {
        BindingFactory bindings = BindingFactory.instantiate();
        BindingContext context = bindings.create("echo", new Configuration())
                .supply(new EchoWorker());

        NamespaceConfig namespace = NamespaceConfig.builder()
            .name("echo")
            .binding()
                .name("echo_server0")
                .type("echo")
                .kind(SERVER)
                .build()
            .build();

        BindingConfig binding = namespace.bindings.stream()
                .filter(b -> "echo_server0".equals(b.name))
                .findFirst()
                .get();

        this.handler = context.attach(binding);
        this.detacher = () -> context.detach(binding);

        this.writeSegment = Arena.ofConfined().allocate(BUFFER_SIZE);
        this.writeBuffer = new UnsafeBuffer(writeSegment.address(), BUFFER_SIZE);
    }

    @TearDown(Level.Trial)
    public void destroy()
    {
        detacher.run();
    }

    @Setup(Level.Iteration)
    public void reset()
    {
    }

    @Benchmark
    public void flyweight(
        final Control control) throws Exception
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(0L)
            .routedId(0L)
            .streamId(0L)
            .sequence(0L)
            .acknowledge(0L)
            .maximum(BUFFER_SIZE)
            .traceId(0L)
            .authorization(0L)
            .affinity(0L)
            .build();

        MessageConsumer sender = MessageConsumer.NOOP;
        MessageConsumer receiver = handler.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), sender);
        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .originId(0L)
                .routedId(0L)
                .streamId(0L)
                .sequence(0L)
                .acknowledge(0L)
                .maximum(BUFFER_SIZE)
                .timestamp(0L)
                .traceId(0L)
                .budgetId(0L)
                .padding(0)
                .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    @Benchmark
    public void varhandle(
        final Control control) throws Exception
    {
        final MemorySegment segment = writeSegment;
        final long beginAt = 0L;
        BeginFields.ORIGIN_ID.set(segment, beginAt, 1L);
        BeginFields.ROUTED_ID.set(segment, beginAt, 0L);
        BeginFields.STREAM_ID.set(segment, beginAt, 3L);
        BeginFields.SEQUENCE.set(segment, beginAt, 4L);
        BeginFields.ACKNOWLEDGE.set(segment, beginAt, 5L);
        BeginFields.TIMESTAMP.set(segment, beginAt, 7L);
        BeginFields.TRACE_ID.set(segment, beginAt, 8L);
        BeginFields.AUTHORIZATION.set(segment, beginAt, 9L);
        BeginFields.AFFINITY.set(segment, beginAt, 10L);
        BeginFields.MAXIMUM.set(segment, beginAt, 6);

        MessageConsumer sender = MessageConsumer.NOOP;
        MessageConsumer receiver = handler.newStream(BeginFW.TYPE_ID, writeBuffer, 0, BeginFields.SIZEOF, sender);

        receiver.accept(BeginFW.TYPE_ID, writeBuffer, (int) beginAt, BeginFields.SIZEOF);

        final long windowAt = BitUtil.align(BeginFields.SIZEOF, 8);
        WindowFields.ORIGIN_ID.set(segment, windowAt, 1L);
        WindowFields.ROUTED_ID.set(segment, windowAt, 0L);
        WindowFields.STREAM_ID.set(segment, windowAt, 3L);
        WindowFields.SEQUENCE.set(segment, windowAt, 4L);
        WindowFields.ACKNOWLEDGE.set(segment, windowAt, 5L);
        WindowFields.TIMESTAMP.set(segment, windowAt, 7L);
        WindowFields.TRACE_ID.set(segment, windowAt, 8L);
        WindowFields.AUTHORIZATION.set(segment, windowAt, 9L);
        WindowFields.BUDGET_ID.set(segment, windowAt, 10L);
        WindowFields.MAXIMUM.set(segment, windowAt, 11);
        WindowFields.MINIMUM.set(segment, windowAt, 12);
        WindowFields.PADDING.set(segment, windowAt, 13);
        WindowFields.CAPABILITIES.set(segment, windowAt, (byte) 0);

        receiver.accept(WindowFW.TYPE_ID, writeBuffer, (int) windowAt, WindowFields.SIZEOF);
    }

    public static void main(
        String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(EchoHandshakeBM.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
