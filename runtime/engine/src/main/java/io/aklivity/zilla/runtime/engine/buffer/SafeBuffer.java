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

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;

public final class SafeBuffer implements DirectBuffer, MutableDirectBuffer, AtomicBuffer
{
    private static final ValueLayout.OfByte BYTE_BE = ValueLayout.JAVA_BYTE.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfShort SHORT_BE = ValueLayout.JAVA_SHORT.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfChar CHAR_BE = ValueLayout.JAVA_CHAR.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfInt INT_BE = ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfLong LONG_BE = ValueLayout.JAVA_LONG.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfFloat FLOAT_BE = ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_BE = ValueLayout.JAVA_DOUBLE.withOrder(ByteOrder.BIG_ENDIAN);

    private static final VarHandle VH_BYTE = BYTE_BE.varHandle();
    private static final VarHandle VH_SHORT = SHORT_BE.varHandle();
    private static final VarHandle VH_CHAR = CHAR_BE.varHandle();
    private static final VarHandle VH_INT = INT_BE.varHandle();
    private static final VarHandle VH_LONG = LONG_BE.varHandle();
    private static final VarHandle VH_FLOAT = FLOAT_BE.varHandle();
    private static final VarHandle VH_DOUBLE = DOUBLE_BE.varHandle();

    private MemorySegment segment;
    private long baseOffset;
    private int capacity;

    public SafeBuffer()
    {
        this.segment = null;
        this.baseOffset = 0L;
        this.capacity = 0;
    }

    public SafeBuffer(
        MemorySegment segment)
    {
        this.segment = Objects.requireNonNull(segment);
        this.baseOffset = 0L;
        this.capacity = (int) segment.byteSize();
    }

    @Override
    public void wrap(
        byte[] buffer,
        int offset,
        int length)
    {
        Objects.requireNonNull(buffer);
        this.segment = MemorySegment.ofBuffer(ByteBuffer.wrap(buffer, offset, length));
        this.baseOffset = 0L;
        this.capacity = length;
    }

    @Override
    public void wrap(ByteBuffer buffer)
    {
        Objects.requireNonNull(buffer);
        this.segment = MemorySegment.ofBuffer(buffer);
        this.baseOffset = 0L;
        this.capacity = buffer.remaining();
    }

    public void wrap(SafeBuffer other, int offset, int length)
    {
        Objects.requireNonNull(other);
        this.segment = other.segment;
        this.baseOffset = other.baseOffset + offset;
        this.capacity = length;
    }

    @Override
    public int capacity()
    {
        return capacity;
    }

    @Override
    public byte getByte(
        int offset)
    {
        return (byte) VH_BYTE.get(segment, baseOffset + offset);
    }

    @Override
    public void putByte(
        int offset,
        byte value)
    {
        VH_BYTE.set(segment, baseOffset + offset, value);
    }

    @Override
    public short getShort(
        int offset)
    {
        return (short) VH_SHORT.get(segment, baseOffset + offset);
    }

    @Override
    public void putShort(
        int offset,
        short value)
    {
        VH_SHORT.set(segment, baseOffset + offset, value);
    }

    @Override
    public char getChar(
        int offset)
    {
        return (char) VH_CHAR.get(segment, baseOffset + offset);
    }

    @Override
    public void putChar(
        int offset,
        char value)
    {
        VH_CHAR.set(segment, baseOffset + offset, value);
    }

    @Override
    public int getInt(
        int offset)
    {
        return (int) VH_INT.get(segment, baseOffset + offset);
    }

    @Override
    public void putInt(
        int offset,
        int value)
    {
        VH_INT.set(segment, baseOffset + offset, value);
    }

    @Override
    public long getLong(
        int offset)
    {
        return (long) VH_LONG.get(segment, baseOffset + offset);
    }

    @Override
    public void putLong(
        int offset,
        long value)
    {
        VH_LONG.set(segment, baseOffset + offset, value);
    }

    @Override
    public float getFloat(
        int offset)
    {
        return (float) VH_FLOAT.get(segment, baseOffset + offset);
    }

    @Override
    public void putFloat(
        int offset,
        float value)
    {
        VH_FLOAT.set(segment, baseOffset + offset, value);
    }

    @Override
    public double getDouble(
        int offset)
    {
        return (double) VH_DOUBLE.get(segment, baseOffset + offset);
    }

    @Override
    public void putDouble(
        int offset,
        double value)
    {
        VH_DOUBLE.set(segment, baseOffset + offset, value);
    }

    @Override
    public byte getByteVolatile(
        int offset)
    {
        return (byte) VH_BYTE.getVolatile(segment, baseOffset + offset);
    }

    @Override
    public void putByteVolatile(
        int offset,
        byte value)
    {
        VH_BYTE.setVolatile(segment, baseOffset + offset, value);
    }

    @Override
    public short getShortVolatile(
        int offset)
    {
        return (short) VH_SHORT.getVolatile(segment, baseOffset + offset);
    }

    @Override
    public void putShortVolatile(
        int offset,
        short value)
    {
        VH_SHORT.setVolatile(segment, baseOffset + offset, value);
    }

    @Override
    public char getCharVolatile(
        int offset)
    {
        return (char) VH_CHAR.getVolatile(segment, baseOffset + offset);
    }

    @Override
    public void putCharVolatile(
        int offset,
        char value)
    {
        VH_CHAR.setVolatile(segment, baseOffset + offset, value);
    }

    @Override
    public int getIntVolatile(
        int offset)
    {
        return (int) VH_INT.getVolatile(segment, baseOffset + offset);
    }

    @Override
    public void putIntVolatile(
        int offset,
        int value)
    {
        VH_INT.setVolatile(segment, baseOffset + offset, value);
    }

    @Override
    public long getLongVolatile(
        int offset)
    {
        return (long) VH_LONG.getVolatile(segment, baseOffset + offset);
    }

    @Override
    public void putLongVolatile(
        int offset,
        long value)
    {
        VH_LONG.setVolatile(segment, baseOffset + offset, value);
    }

    public void putByteOrdered(
        int offset,
        byte value)
    {
        VH_BYTE.setRelease(segment, baseOffset + offset, value);
    }

    public void putShortOrdered(
        int offset,
        short value)
    {
        VH_SHORT.setRelease(segment, baseOffset + offset, value);
    }

    public void putCharOrdered(
        int offset,
        char value)
    {
        VH_CHAR.setRelease(segment, baseOffset + offset, value);
    }

    @Override
    public void putIntOrdered(
        int offset,
        int value)
    {
        VH_INT.setRelease(segment, baseOffset + offset, value);
    }

    @Override
    public void putLongOrdered(
        int offset,
        long value)
    {
        VH_LONG.setRelease(segment, baseOffset + offset, value);
    }

    @Override
    public boolean compareAndSetInt(
        int offset,
        int expected,
        int value)
    {
        return VH_INT.compareAndSet(segment, baseOffset + offset, expected, value);
    }

    @Override
    public boolean compareAndSetLong(
        int offset,
        long expected,
        long value)
    {
        return VH_LONG.compareAndSet(segment, baseOffset + offset, expected, value);
    }

    @Override
    public int getAndSetInt(
        int offset,
        int value)
    {
        return (int) VH_INT.getAndSet(segment, baseOffset + offset, value);
    }

    @Override
    public long getAndSetLong(
        int offset,
        long value)
    {
        return (long) VH_LONG.getAndSet(segment, baseOffset + offset, value);
    }

    @Override
    public int getAndAddInt(
        int offset,
        int delta)
    {
        return (int) VH_INT.getAndAdd(segment, baseOffset + offset, delta);
    }

    @Override
    public long getAndAddLong(
        int offset,
        long delta)
    {
        return (long) VH_LONG.getAndAdd(segment, baseOffset + offset, delta);
    }

    @Override
    public int addIntOrdered(
        int offset,
        int delta)
    {
        return (int) VH_INT.getAndAdd(segment, baseOffset + offset, delta);
    }

    @Override
    public long addLongOrdered(
        int offset,
        long delta)
    {
        return (long) VH_LONG.getAndAdd(segment, baseOffset + offset, delta);
    }

    @Override
    public void getBytes(
        int offset,
        byte[] dst,
        int dstOffset,
        int length)
    {
        MemorySegment.ofBuffer(ByteBuffer.wrap(dst, dstOffset, length))
            .copyFrom(segment.asSlice(baseOffset + offset, length));
    }

    @Override
    public void putBytes(
        int offset,
        byte[] src,
        int srcOffset,
        int length)
    {
        segment.asSlice(baseOffset + offset, length)
                .copyFrom(MemorySegment.ofBuffer(ByteBuffer.wrap(src, srcOffset, length)));
    }

    @Override
    public int compareTo(
        DirectBuffer o)
    {
        long len = Math.min(this.capacity, o.capacity());
        for (int i = 0; i < len; i++)
        {
            int cmp = Byte.compare(this.getByte(i), o.getByte(i));
            if (cmp != 0)
            {
                return cmp;
            }
        }
        return Long.compare(this.capacity, o.capacity());
    }

    @Override
    public void verifyAlignment()
    {
    }

    @Override
    public boolean isExpandable()
    {
        return false;
    }

    @Override
    public void setMemory(
        int index,
        int length,
        byte value)
    {
        segment.asSlice(baseOffset + index, length).fill(value);
    }

    @Override
    public long addressOffset()
    {
        return 0;
    }

    @Override
    public byte[] byteArray()
    {
        return null;
    }

    @Override
    public ByteBuffer byteBuffer()
    {
        return null;
    }

    @Override
    public int putStringAscii(
        int index,
        String value)
    {
        byte[] bytes = value.getBytes(US_ASCII);
        putBytes(index, bytes, 0, bytes.length);
        return bytes.length;
    }

    @Override
    public int putStringAscii(
        int index,
        CharSequence value)
    {
        return putStringAscii(index, value.toString());
    }

    @Override
    public int putStringAscii(
        int index,
        String value,
        ByteOrder byteOrder)
    {
        return putStringAscii(index, value);
    }

    @Override
    public int putStringAscii(
        int index,
        CharSequence value,
        ByteOrder byteOrder)
    {
        return putStringAscii(index, value.toString(), byteOrder);
    }

    @Override
    public int putStringWithoutLengthAscii(
        int index,
        String value)
    {
        return putStringAscii(index, value);
    }

    @Override
    public int putStringWithoutLengthAscii(
        int index,
        CharSequence value)
    {
        return putStringWithoutLengthAscii(index, value.toString());
    }

    @Override
    public int putStringWithoutLengthAscii(
        int index,
        String value,
        int valueOffset,
        int length)
    {
        byte[] bytes = value.substring(valueOffset, valueOffset + length).getBytes(US_ASCII);
        putBytes(index, bytes, 0, length);
        return length;
    }

    @Override
    public int putStringWithoutLengthAscii(
        int index,
        CharSequence value,
        int valueOffset,
        int length)
    {
        return putStringWithoutLengthAscii(index, value.toString(), valueOffset, length);
    }

    @Override
    public String getStringAscii(
        int index)
    {
        int length = 0;
        while (getByte(index + length) != 0)
        {
            length++;
        }
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return new String(bytes, US_ASCII);
    }

    @Override
    public int getStringAscii(
        int index, Appendable appendable)
    {
        int length = 0;
        while (getByte(index + length) != 0)
        {
            length++;
        }
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        try
        {
            appendable.append(new String(bytes, US_ASCII));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return length;
    }

    @Override
    public String getStringAscii(
        int index,
        ByteOrder byteOrder)
    {
        return getStringAscii(index);
    }

    @Override
    public int getStringAscii(
        int index,
        Appendable appendable,
        ByteOrder byteOrder)
    {
        return getStringAscii(index, appendable);
    }

    @Override
    public String getStringAscii(
        int index,
        int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return new String(bytes, US_ASCII);
    }

    @Override
    public int getStringAscii(
        int index,
        int length,
        Appendable appendable)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        try
        {
            appendable.append(new String(bytes, US_ASCII));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        return length;
    }

    @Override
    public String getStringWithoutLengthAscii(
        int index,
        int length)
    {
        return getStringAscii(index, length);
    }

    @Override
    public int getStringWithoutLengthAscii(
        int index,
        int length,
        Appendable appendable)
    {
        return getStringAscii(index, length, appendable);
    }

    @Override
    public int putNaturalIntAscii(
        int index,
        int value)
    {
        byte[] buf = Integer.toString(value).getBytes(US_ASCII);
        putBytes(index, buf, 0, buf.length);
        return buf.length;
    }

    @Override
    public int putNaturalLongAscii(
        int index,
        long value)
    {
        byte[] buf = Long.toString(value).getBytes(US_ASCII);
        putBytes(index, buf, 0, buf.length);
        return buf.length;
    }

    @Override
    public void putNaturalPaddedIntAscii(
        int index,
        int length,
        int value)
    {
        String s = Integer.toString(value);
        if (s.length() > length)
        {
            throw new NumberFormatException("Value too large to fit");
        }
        String padded = "0".repeat(length - s.length()) + s;
        putStringWithoutLengthAscii(index, padded);
    }

    @Override
    public int putNaturalIntAsciiFromEnd(
        int value,
        int endExclusive)
    {
        String s = Integer.toString(value);
        int start = endExclusive - s.length();
        putStringWithoutLengthAscii(start, s);
        return s.length();
    }

    @Override
    public int putStringUtf8(
        int index,
        String value,
        int maxEncodedLength)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > maxEncodedLength)
        {
            throw new IllegalArgumentException("Encoded length exceeds max");
        }
        putBytes(index, bytes, 0, bytes.length);
        return bytes.length;
    }

    @Override
    public int putStringUtf8(
        int index,
        String value,
        ByteOrder byteOrder,
        int maxEncodedLength)
    {
        return putStringUtf8(index, value, maxEncodedLength);
    }

    @Override
    public int putStringUtf8(
        int index,
        String value,
        ByteOrder byteOrder)
    {
        return putStringUtf8(index, value);
    }

    @Override
    public int putStringUtf8(
        int index, String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        putBytes(index, bytes, 0, bytes.length);
        return bytes.length;
    }

    @Override
    public int putStringWithoutLengthUtf8(
        int index,
        String value)
    {
        return putStringUtf8(index, value);
    }

    @Override
    public String getStringUtf8(
        int index,
        int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes, 0, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public String getStringUtf8(
        int index)
    {
        int length = 0;
        while (getByte(index + length) != 0)
        {
            length++;
        }
        return getStringUtf8(index, length);
    }

    @Override
    public String getStringUtf8(
        int index,
        ByteOrder byteOrder)
    {
        return getStringUtf8(index);
    }

    @Override
    public String getStringWithoutLengthUtf8(
        int index,
        int length)
    {
        return getStringUtf8(index, length);
    }

    @Override
    public int getInt(
        int index,
        ByteOrder byteOrder)
    {
        int val = getInt(index);
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            val = Integer.reverseBytes(val);
        }
        return val;
    }

    @Override
    public void putInt(
        int index,
        int value,
        ByteOrder byteOrder)
    {
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            value = Integer.reverseBytes(value);
        }
        putInt(index, value);
    }

    @Override
    public long getLong(
        int index,
        ByteOrder byteOrder)
    {
        long val = getLong(index);
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            val = Long.reverseBytes(val);
        }
        return val;
    }

    @Override
    public void putLong(
        int index,
        long value,
        ByteOrder byteOrder)
    {
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            value = Long.reverseBytes(value);
        }
        putLong(index, value);
    }

    @Override
    public short getShort(
        int index,
        ByteOrder byteOrder)
    {
        short val = getShort(index);
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            val = Short.reverseBytes(val);
        }
        return val;
    }

    @Override
    public void putShort(
        int index,
        short value,
        ByteOrder byteOrder)
    {
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            value = Short.reverseBytes(value);
        }
        putShort(index, value);
    }

    @Override
    public char getChar(
        int index,
        ByteOrder byteOrder)
    {
        char val = getChar(index);
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            val = Character.reverseBytes(val);
        }
        return val;
    }

    @Override
    public void putChar(
        int index,
        char value,
        ByteOrder byteOrder)
    {
        if (byteOrder != ByteOrder.BIG_ENDIAN)
        {
            value = Character.reverseBytes(value);
        }
        putChar(index, value);
    }

    @Override
    public float getFloat(
        int index,
        ByteOrder byteOrder)
    {
        int intBits = getInt(index, byteOrder);
        return Float.intBitsToFloat(intBits);
    }

    @Override
    public void putFloat(
        int index,
        float value,
        ByteOrder byteOrder)
    {
        int intBits = Float.floatToIntBits(value);
        putInt(index, intBits, byteOrder);
    }

    @Override
    public double getDouble(
        int index,
        ByteOrder byteOrder)
    {
        long longBits = getLong(index, byteOrder);
        return Double.longBitsToDouble(longBits);
    }

    @Override
    public void putDouble(
        int index,
        double value,
        ByteOrder byteOrder)
    {
        long longBits = Double.doubleToLongBits(value);
        putLong(index, longBits, byteOrder);
    }

    @Override
    public void wrap(
        byte[] buffer)
    {
        this.segment = MemorySegment.ofArray(buffer);
        this.baseOffset = 0;
        this.capacity = buffer.length;
    }

    @Override
    public void wrap(
        ByteBuffer buffer,
        int offset,
        int length)
    {
        this.segment = MemorySegment.ofBuffer(buffer.slice(offset, length));
        this.baseOffset = 0;
        this.capacity = length;
    }

    @Override
    public void wrap(DirectBuffer buffer)
    {
        if (buffer instanceof SafeBuffer sb)
        {
            this.segment = sb.segment;
            this.baseOffset = sb.baseOffset;
            this.capacity = sb.capacity;
        }
        else
        {
            throw new UnsupportedOperationException("Wrap from non-SafeBuffer DirectBuffer is not supported yet");
        }
    }

    @Override
    public void wrap(
        DirectBuffer buffer,
        int offset,
        int length)
    {
        if (buffer instanceof SafeBuffer sb)
        {
            this.segment = sb.segment;
            this.baseOffset = sb.baseOffset + offset;
            this.capacity = length;
        }
        else
        {
            throw new UnsupportedOperationException("Wrap from non-SafeBuffer DirectBuffer is not supported yet");
        }
    }

    @Override
    public void wrap(
        long address,
        int length)
    {
        throw new UnsupportedOperationException("Wrapping raw address not supported with MemorySegment");
    }

    @Override
    public int wrapAdjustment()
    {
        return 0;
    }

    @Override
    public void putBytes(
        int index,
        byte[] src)
    {
        putBytes(index, src, 0, src.length);
    }

    @Override
    public void putBytes(
        int index,
        ByteBuffer srcBuffer,
        int length)
    {
        ByteBuffer slice = srcBuffer.duplicate();
        slice.limit(length);
        segment.asSlice(baseOffset + index, length).copyFrom(MemorySegment.ofBuffer(slice));
    }

    @Override
    public void putBytes(
        int index,
        ByteBuffer srcBuffer,
        int srcIndex,
        int length)
    {
        ByteBuffer slice = srcBuffer.duplicate();
        slice.position(srcIndex).limit(srcIndex + length);
        segment.asSlice(baseOffset + index, length).copyFrom(MemorySegment.ofBuffer(slice));
    }

    @Override
    public void putBytes(
        int index,
        DirectBuffer srcBuffer,
        int srcIndex,
        int length)
    {
        byte[] temp = new byte[length];
        srcBuffer.getBytes(srcIndex, temp, 0, length);
        putBytes(index, temp);
    }

    @Override
    public void getBytes(
        int index, byte[] dst)
    {
        getBytes(index, dst, 0, dst.length);
    }

    @Override
    public void getBytes(
        int index,
        MutableDirectBuffer dstBuffer,
        int dstIndex,
        int length)
    {
        byte[] temp = new byte[length];
        getBytes(index, temp);
        dstBuffer.putBytes(dstIndex, temp);
    }

    @Override
    public void getBytes(
        int index,
        ByteBuffer dstBuffer,
        int length)
    {
        ByteBuffer slice = dstBuffer.duplicate();
        slice.limit(length);
        MemorySegment.ofBuffer(slice).copyFrom(segment.asSlice(baseOffset + index, length));
    }

    @Override
    public void getBytes(
        int index,
        ByteBuffer dstBuffer,
        int dstOffset,
        int length)
    {
        ByteBuffer slice = dstBuffer.duplicate();
        slice.position(dstOffset).limit(dstOffset + length);
        MemorySegment.ofBuffer(slice).copyFrom(segment.asSlice(baseOffset + index, length));
    }

    @Override
    public int putIntAscii(
        int index,
        int value)
    {
        byte[] bytes = Integer.toString(value).getBytes(StandardCharsets.US_ASCII);
        putBytes(index, bytes);
        return bytes.length;
    }

    @Override
    public int putLongAscii(
        int index,
        long value)
    {
        byte[] bytes = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        putBytes(index, bytes);
        return bytes.length;
    }

    @Override
    public int parseNaturalIntAscii(
        int index,
        int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes);
        return Integer.parseInt(new String(bytes, StandardCharsets.US_ASCII));
    }

    @Override
    public long parseNaturalLongAscii(
        int index,
        int length)
    {
        byte[] bytes = new byte[length];
        getBytes(index, bytes);
        return Long.parseLong(new String(bytes, StandardCharsets.US_ASCII));
    }

    @Override
    public int parseIntAscii(
        int index,
        int length)
    {
        return parseNaturalIntAscii(index, length);
    }

    @Override
    public long parseLongAscii(
        int index,
        int length)
    {
        return parseNaturalLongAscii(index, length);
    }

    @Override
    public void checkLimit(
        int limit)
    {
        if (limit > capacity)
        {
            throw new IndexOutOfBoundsException("Limit exceeds buffer capacity");
        }
    }

    @Override
    public void boundsCheck(
        int index,
        int length)
    {
        if (index < 0 || length < 0 || index + length > capacity)
        {
            throw new IndexOutOfBoundsException("Buffer bounds exceeded");
        }
    }
}
