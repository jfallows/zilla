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
package io.aklivity.zilla.runtime.binding.echo.internal.types.stream;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

public final class WindowFields
{
    public static final VarHandle ORIGIN_ID;
    public static final VarHandle ROUTED_ID;
    public static final VarHandle STREAM_ID;
    public static final VarHandle SEQUENCE;
    public static final VarHandle ACKNOWLEDGE;
    public static final VarHandle TIMESTAMP;
    public static final VarHandle TRACE_ID;
    public static final VarHandle AUTHORIZATION;
    public static final VarHandle BUDGET_ID;
    public static final VarHandle MAXIMUM;
    public static final VarHandle MINIMUM;
    public static final VarHandle PADDING;
    public static final VarHandle CAPABILITIES;

    public static final int SIZEOF;

    static
    {
        MemoryLayout layout = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("originId"),
            ValueLayout.JAVA_LONG.withName("routedId"),
            ValueLayout.JAVA_LONG.withName("streamId"),
            ValueLayout.JAVA_LONG.withName("sequence"),
            ValueLayout.JAVA_LONG.withName("acknowledge"),
            ValueLayout.JAVA_LONG.withName("timestamp"),
            ValueLayout.JAVA_LONG.withName("traceId"),
            ValueLayout.JAVA_LONG.withName("authorization"),
            ValueLayout.JAVA_LONG.withName("budgetId"),
            ValueLayout.JAVA_INT.withName("maximum"),
            ValueLayout.JAVA_INT.withName("minimum"),
            ValueLayout.JAVA_INT.withName("padding"),
            ValueLayout.JAVA_BYTE.withName("capabilities"));

        ORIGIN_ID = layout.varHandle(groupElement("originId"));
        ROUTED_ID = layout.varHandle(groupElement("routedId"));
        STREAM_ID = layout.varHandle(groupElement("streamId"));
        SEQUENCE = layout.varHandle(groupElement("sequence"));
        ACKNOWLEDGE = layout.varHandle(groupElement("acknowledge"));
        MAXIMUM = layout.varHandle(groupElement("maximum"));
        TIMESTAMP = layout.varHandle(groupElement("timestamp"));
        TRACE_ID = layout.varHandle(groupElement("traceId"));
        AUTHORIZATION = layout.varHandle(groupElement("authorization"));
        BUDGET_ID = layout.varHandle(groupElement("budgetId"));
        MINIMUM = layout.varHandle(groupElement("minimum"));
        PADDING = layout.varHandle(groupElement("padding"));
        CAPABILITIES = layout.varHandle(groupElement("capabilities"));

        SIZEOF = (int) (layout.byteOffset(groupElement("capabilities")) + ValueLayout.JAVA_BYTE.byteSize());
    }

    private WindowFields()
    {
    }
}
