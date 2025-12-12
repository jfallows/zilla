/*
 * Copyright 2021-2024 Aklivity Inc.
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
package io.aklivity.zilla.runtime.engine.buffer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class SafeBufferTest
{
    @Test
    public void shouldCreateEmptyBuffer()
    {
        SafeBuffer buffer = new SafeBuffer();

        assertThat(buffer.capacity(), equalTo(0));
    }

    @Test
    public void shouldCreateNonEmptyBuffer()
    {
        int capacity = new Random().nextInt(256);
        MemorySegment segment = Arena.global().allocate(capacity);
        SafeBuffer buffer = new SafeBuffer(segment);

        assertThat(buffer.capacity(), equalTo(capacity));
    }

    @Test
    public void shouldWrapByteArray()
    {
        int capacity = new Random().nextInt(256);
        byte[] array = new byte[capacity];
        Arrays.fill(array, (byte) 0x01);
        MemorySegment segment = MemorySegment.ofArray(array);
        SafeBuffer buffer = new SafeBuffer(segment);

        assertThat(buffer.capacity(), equalTo(capacity));
        assertThat(buffer.getByte(0), equalTo((byte) 0x01));
    }
}
