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
package io.aklivity.zilla.runtime.binding.openapi.internal.streams;

import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import io.aklivity.zilla.runtime.binding.openapi.internal.OpenapiBinding;
import io.aklivity.zilla.runtime.binding.openapi.internal.OpenapiConfiguration;
import io.aklivity.zilla.runtime.binding.openapi.internal.config.OpenapiBindingConfig;
import io.aklivity.zilla.runtime.binding.openapi.internal.config.OpenapiRouteConfig;
import io.aklivity.zilla.runtime.binding.openapi.internal.config.composite.OpenapiServerNamespaceGenerator;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.Flyweight;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.OctetsFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.AbortFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.BeginFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.DataFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.EndFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.FlushFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.HttpBeginExFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.OpenapiBeginExFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.ResetFW;
import io.aklivity.zilla.runtime.binding.openapi.internal.types.stream.WindowFW;
import io.aklivity.zilla.runtime.engine.EngineContext;
import io.aklivity.zilla.runtime.engine.binding.BindingHandler;
import io.aklivity.zilla.runtime.engine.binding.function.MessageConsumer;
import io.aklivity.zilla.runtime.engine.buffer.BufferPool;
import io.aklivity.zilla.runtime.engine.catalog.CatalogHandler;
import io.aklivity.zilla.runtime.engine.config.BindingConfig;
import io.aklivity.zilla.runtime.engine.config.NamespaceConfig;

public final class OpenapiServerFactory implements OpenapiStreamFactory
{
    private static final String HTTP_TYPE_NAME = "http";
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(), 0, 0);

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final FlushFW flushRO = new FlushFW();
    private final AbortFW abortRO = new AbortFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();

    private final OpenapiBeginExFW openapiBeginExRO = new OpenapiBeginExFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();

    private final OpenapiBeginExFW.Builder openapiBeginExRW = new OpenapiBeginExFW.Builder();

    private final OpenapiConfiguration config;
    private final OpenapiServerNamespaceGenerator namespaceGenerator;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final BufferPool bufferPool;
    private final BindingHandler streamFactory;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final LongFunction<CatalogHandler> supplyCatalog;
    private final Consumer<NamespaceConfig> attachComposite;
    private final Consumer<NamespaceConfig> detachComposite;
    private final Long2ObjectHashMap<OpenapiBindingConfig> bindings;
    private final int openapiTypeId;
    private final int httpTypeId;

    public OpenapiServerFactory(
        OpenapiConfiguration config,
        EngineContext context)
    {
        this.config = config;
        this.writeBuffer = context.writeBuffer();
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = context.bufferPool();
        this.streamFactory = context.streamFactory();
        this.supplyInitialId = context::supplyInitialId;
        this.supplyReplyId = context::supplyReplyId;
        this.supplyTraceId = context::supplyTraceId;
        this.supplyCatalog = context::supplyCatalog;
        this.attachComposite = context::attachComposite;
        this.detachComposite = context::detachComposite;
        this.namespaceGenerator = new OpenapiServerNamespaceGenerator();
        this.bindings = new Long2ObjectHashMap<>();
        this.openapiTypeId = context.supplyTypeId(OpenapiBinding.NAME);
        this.httpTypeId = context.supplyTypeId(HTTP_TYPE_NAME);
    }

    @Override
    public int originTypeId()
    {
        return httpTypeId;
    }

    @Override
    public int routedTypeId()
    {
        return openapiTypeId;
    }

    @Override
    public void attach(
        BindingConfig binding)
    {
        OpenapiBindingConfig openapiBinding = new OpenapiBindingConfig(binding, namespaceGenerator, supplyCatalog,
            attachComposite, detachComposite, config.targetRouteId());
        bindings.put(binding.id, openapiBinding);

        openapiBinding.attach(binding);
    }

    @Override
    public void detach(
        long bindingId)
    {
        OpenapiBindingConfig openapiBinding = bindings.remove(bindingId);
        openapiBinding.detach();
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer receiver)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long originId = begin.originId();
        final long routedId = begin.routedId();
        final long initialId = begin.streamId();
        final long affinity = begin.affinity();
        final long authorization = begin.authorization();
        final OctetsFW extension = begin.extension();
        final HttpBeginExFW httpBeginEx = extension.get(httpBeginExRO::tryWrap);

        final OpenapiBindingConfig binding = bindings.get(routedId);

        MessageConsumer newStream = null;

        if (binding != null && binding.isCompositeOriginId(originId))
        {
            final OpenapiRouteConfig route = binding.resolve(authorization);

            if (route != null)
            {
                final String operationId = binding.resolveOperationId(httpBeginEx);
                final long apiId = binding.resolveApiId(originId);

                newStream = new HttpStream(
                    receiver,
                    originId,
                    routedId,
                    initialId,
                    affinity,
                    authorization,
                    route.id,
                    apiId,
                    operationId)::onHttpMessage;
            }
        }

        return newStream;
    }

    private final class HttpStream
    {
        private final OpenapiStream openapi;
        private final MessageConsumer sender;
        private final long originId;
        private final long routedId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;

        private int state;

        private HttpStream(
            MessageConsumer sender,
            long originId,
            long routedId,
            long initialId,
            long affinity,
            long authorization,
            long resolvedId,
            long apiId,
            String operationId)
        {
            this.openapi =  new OpenapiStream(this, routedId, resolvedId, authorization, apiId, operationId);
            this.sender = sender;
            this.originId = originId;
            this.routedId = routedId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.authorization = authorization;
        }

        private void onHttpMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onHttpBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onHttpData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onHttpEnd(end);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onHttpFlush(flush);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onHttpAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onHttpWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onHttpReset(reset);
                break;
            default:
                break;
            }
        }

        private void onHttpBegin(
            BeginFW begin)
        {
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();
            final int maximum = begin.maximum();
            final long traceId = begin.traceId();
            final long affinity = begin.affinity();
            final OctetsFW extension = begin.extension();

            assert acknowledge <= sequence;

            state = OpenapiState.openingInitial(state);

            openapi.doOpenapiBegin(sequence, acknowledge, maximum, traceId, affinity, extension);
        }

        private void onHttpData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final int maximum = data.maximum();
            final long traceId = data.traceId();
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final int flags = data.flags();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            assert acknowledge <= sequence;

            openapi.doOpenapiData(sequence, acknowledge, maximum, traceId, authorization, budgetId,
                reserved, flags, payload, extension);
        }

        private void onHttpEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final int maximum = end.maximum();
            final long traceId = end.traceId();
            final OctetsFW extension = end.extension();

            assert acknowledge <= sequence;

            state = OpenapiState.closeInitial(state);

            openapi.doOpenapiEnd(sequence, acknowledge, maximum, traceId, extension);
        }

        private void onHttpFlush(
            FlushFW flush)
        {
            final long sequence = flush.sequence();
            final long acknowledge = flush.acknowledge();
            final int maximum = flush.maximum();
            final long traceId = flush.traceId();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();

            assert acknowledge <= sequence;

            openapi.doOpenapiFlush(sequence, acknowledge, maximum, traceId, budgetId, reserved, extension);
        }

        private void onHttpAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final int maximum = abort.maximum();
            final long traceId = abort.traceId();
            final OctetsFW extension = abort.extension();

            assert acknowledge <= sequence;

            state = OpenapiState.closeInitial(state);

            openapi.doOpenapiAbort(sequence, acknowledge, maximum, traceId, extension);
        }

        private void onHttpReset(
            ResetFW reset)
        {
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final int maximum = reset.maximum();
            final long traceId = reset.traceId();

            assert acknowledge <= sequence;

            state = OpenapiState.closeReply(state);

            openapi.doOpenapiReset(sequence, acknowledge, maximum, traceId);
        }

        private void onHttpWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            assert acknowledge <= sequence;

            state = OpenapiState.closingReply(state);

            openapi.doOpenapiWindow(sequence, acknowledge, maximum, traceId, authorization, budgetId, padding);
        }

        private void doHttpBegin(
            long traceId,
            long sequence,
            long acknowledge,
            int maximum,
            OctetsFW extension)
        {
            state = OpenapiState.openingReply(state);

            doBegin(sender, originId, routedId, replyId, sequence, acknowledge, maximum,
                traceId, authorization, affinity, extension);
        }

        private void doHttpData(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            int flag,
            int reserved,
            long replyBudgetId,
            OctetsFW payload,
            Flyweight extension)
        {
            doData(sender, originId, routedId, replyId, sequence, acknowledge, maximum,
                    traceId, authorization, replyBudgetId, flag, reserved, payload, extension);
        }

        private void doHttpFlush(
            long traceId,
            long replyBudgetId,
            long sequence,
            long acknowledge,
            int maximum,
            int reserved,
            OctetsFW extension)
        {
            doFlush(sender, originId, routedId, replyId, sequence, acknowledge, maximum,
                traceId, authorization, replyBudgetId, reserved, extension);
        }

        private void doHttpEnd(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            OctetsFW extension)
        {
            if (OpenapiState.replyOpening(state) && !OpenapiState.replyClosed(state))
            {
                doEnd(sender, originId, routedId, replyId, sequence, acknowledge, maximum,
                    traceId, authorization, extension);
            }

            state = OpenapiState.closeReply(state);
        }

        private void doHttpAbort(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId)
        {
            if (OpenapiState.replyOpening(state) && !OpenapiState.replyClosed(state))
            {
                doAbort(sender, originId, routedId, replyId, sequence, acknowledge, maximum,
                    traceId, authorization, EMPTY_OCTETS);
            }

            state = OpenapiState.closeInitial(state);
        }

        private void doHttpReset(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId)
        {
            if (!OpenapiState.initialClosed(state))
            {
                state = OpenapiState.closeInitial(state);

                doReset(sender, originId, routedId, initialId, sequence, acknowledge, maximum,
                    traceId, authorization, EMPTY_OCTETS);
            }
        }

        private void doHttpWindow(
            long sequence,
            long acknowledge,
            int maximum,
            long authorization,
            long traceId,
            long budgetId,
            int padding)
        {
            doWindow(sender, originId, routedId, initialId, sequence, acknowledge, maximum,
                traceId, authorization, budgetId, padding);
        }
    }

    private final class OpenapiStream
    {
        private final HttpStream delegate;
        private final String operationId;
        private final long originId;
        private final long routedId;
        private final long apiId;
        private final long authorization;

        private final long initialId;
        private final long replyId;
        private MessageConsumer receiver;

        private int state;

        private OpenapiStream(
            HttpStream delegate,
            long originId,
            long routedId,
            long authorization,
            long apiId,
            String operationId)
        {
            this.delegate = delegate;
            this.originId = originId;
            this.routedId = routedId;
            this.apiId = apiId;
            this.receiver = MessageConsumer.NOOP;
            this.authorization = authorization;
            this.initialId = supplyInitialId.applyAsLong(routedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.operationId = operationId;
        }

        private void onOpenapiMessage(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onOpenapiBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onOpenapiData(data);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onOpenapiFlush(flush);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onOpenapiEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onOpenapiAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onOpenapiReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onOpenapiWindow(window);
                break;
            default:
                break;
            }
        }

        private void onOpenapiBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();
            final int maximum = begin.maximum();
            final OctetsFW extension = begin.extension();

            state = OpenapiState.openingReply(state);

            final OpenapiBeginExFW beginEx = extension.get(openapiBeginExRO::tryWrap);
            OctetsFW openapiEx = beginEx != null ? beginEx.extension() : EMPTY_OCTETS;

            delegate.doHttpBegin(traceId, sequence, acknowledge, maximum, openapiEx);
        }

        private void onOpenapiData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final int maximum = data.maximum();
            final long traceId = data.traceId();
            final int flags = data.flags();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();
            final OctetsFW payload = data.payload();
            final OctetsFW extension = data.extension();

            assert acknowledge <= sequence;

            delegate.doHttpData(sequence, acknowledge, maximum, traceId, flags, reserved, budgetId, payload, extension);
        }

        private void onOpenapiFlush(
            FlushFW flush)
        {
            final long budgetId = flush.budgetId();
            final long sequence = flush.sequence();
            final long acknowledge = flush.acknowledge();
            final int maximum = flush.maximum();
            final long traceId = flush.traceId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();

            assert acknowledge <= sequence;

            delegate.doHttpFlush(traceId, budgetId, sequence, acknowledge, maximum, reserved, extension);
        }

        private void onOpenapiEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final int maximum = end.maximum();
            final long traceId = end.traceId();
            final OctetsFW extension = end.extension();

            assert acknowledge <= sequence;

            state = OpenapiState.closingReply(state);

            delegate.doHttpEnd(sequence, acknowledge, maximum, traceId, extension);
        }

        private void onOpenapiAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final int maximum = abort.maximum();
            final long traceId = abort.traceId();

            assert acknowledge <= sequence;

            state = OpenapiState.closingReply(state);

            delegate.doHttpAbort(sequence, acknowledge, maximum, traceId);
        }

        private void onOpenapiReset(
            ResetFW reset)
        {
            final long sequence = reset.sequence();
            final long acknowledge = reset.acknowledge();
            final int maximum = reset.maximum();
            final long traceId = reset.traceId();

            assert acknowledge <= sequence;

            state = OpenapiState.closeInitial(state);

            delegate.doHttpReset(sequence, acknowledge, maximum, traceId);
        }


        private void onOpenapiWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final int maximum = window.maximum();
            final long authorization = window.authorization();
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            assert acknowledge <= sequence;

            state = OpenapiState.openInitial(state);

            delegate.doHttpWindow(sequence, acknowledge, maximum, authorization, traceId, budgetId, padding);
        }

        private void doOpenapiBegin(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            long affinity,
            OctetsFW extension)
        {
            if (!OpenapiState.initialOpening(state))
            {
                assert state == 0;

                final OpenapiBeginExFW openapiBeginEx = openapiBeginExRW
                    .wrap(extBuffer, 0, extBuffer.capacity())
                    .typeId(openapiTypeId)
                    .apiId(apiId)
                    .operationId(operationId)
                    .extension(extension)
                    .build();

                this.receiver = newStream(this::onOpenapiMessage, originId, routedId, initialId, sequence,
                    acknowledge, maximum, traceId, authorization, affinity, openapiBeginEx);
                state = OpenapiState.openingInitial(state);
            }
        }

        private void doOpenapiData(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            int flags,
            OctetsFW payload,
            Flyweight extension)
        {
            doData(receiver, originId, routedId, initialId, sequence, acknowledge, maximum,
                traceId, authorization, budgetId, flags, reserved, payload, extension);
        }

        private void doOpenapiFlush(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            long initialBud,
            int reserved,
            OctetsFW extension)
        {
            doFlush(receiver, originId, routedId, initialId, sequence, acknowledge, maximum,
                traceId, authorization, initialBud, reserved, extension);

            sequence += reserved;

            assert sequence <= acknowledge + maximum;
        }

        private void doOpenapiEnd(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            OctetsFW extension)
        {
            if (!OpenapiState.initialClosed(state))
            {
                doEnd(receiver, originId, routedId, initialId, sequence, acknowledge, maximum,
                    traceId, authorization, extension);

                state = OpenapiState.closeInitial(state);
            }
        }

        private void doOpenapiAbort(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            OctetsFW extension)
        {
            if (!OpenapiState.initialClosed(state))
            {
                doAbort(receiver, originId, routedId, initialId, sequence, acknowledge, maximum,
                    traceId, authorization, extension);

                state = OpenapiState.closeInitial(state);
            }
        }

        private void doOpenapiReset(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId)
        {
            if (!OpenapiState.replyClosed(state))
            {
                doReset(receiver, originId, routedId, replyId, sequence, acknowledge, maximum,
                    traceId, authorization, EMPTY_OCTETS);

                state = OpenapiState.closeReply(state);
            }
        }

        private void doOpenapiWindow(
            long sequence,
            long acknowledge,
            int maximum,
            long traceId,
            long authorization,
            long budgetId,
            int padding)
        {
            doWindow(receiver, originId, routedId, replyId, sequence, acknowledge, maximum,
                traceId, authorization, budgetId, padding);
        }
    }

    private MessageConsumer newStream(
        MessageConsumer sender,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .originId(originId)
                .routedId(routedId)
                .streamId(streamId)
                .sequence(sequence)
                .acknowledge(acknowledge)
                .maximum(maximum)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        final MessageConsumer receiver =
                streamFactory.newStream(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof(), sender);

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

        return receiver;
    }

    private void doBegin(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .affinity(affinity)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int flags,
        int reserved,
        OctetsFW payload,
        Flyweight extension)
    {
        final DataFW frame = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .flags(flags)
            .budgetId(budgetId)
            .reserved(reserved)
            .payload(payload)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(frame.typeId(), frame.buffer(), frame.offset(), frame.sizeof());
    }

    private void doFlush(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        OctetsFW extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .budgetId(budgetId)
            .reserved(reserved)
            .extension(extension)
            .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension)
            .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        OctetsFW extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension)
            .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .budgetId(budgetId)
            .padding(padding)
            .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long originId,
        long routedId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .originId(originId)
            .routedId(routedId)
            .streamId(streamId)
            .sequence(sequence)
            .acknowledge(acknowledge)
            .maximum(maximum)
            .traceId(traceId)
            .authorization(authorization)
            .extension(extension.buffer(), extension.offset(), extension.sizeof())
            .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }
}
