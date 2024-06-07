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
package io.aklivity.zilla.runtime.binding.tcp.internal.config;

import static java.net.StandardSocketOptions.SO_REUSEADDR;
import static java.net.StandardSocketOptions.SO_REUSEPORT;
import static org.agrona.CloseHelper.quietClose;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;

import org.agrona.LangUtil;

import io.aklivity.zilla.runtime.binding.tcp.config.TcpOptionsConfig;

public final class TcpServerBindingConfig
{
    public final long id;

    private SelectableChannel[] channels;

    public TcpServerBindingConfig(
        long bindingId)
    {
        this.id = bindingId;
    }

    public SelectableChannel[] bind(
        TcpOptionsConfig options)
    {
        try
        {
            assert channels == null;

            int size = options.ports != null ? options.ports.length : 0;
            channels = new SelectableChannel[size];

            for (int i = 0; i < size; i++)
            {
                InetAddress address = InetAddress.getByName(options.host);
                InetSocketAddress local = new InetSocketAddress(address, options.ports[i]);

                channels[i] = ServerSocketChannel.open()
                    .setOption(SO_REUSEADDR, true)
                    .setOption(SO_REUSEPORT, true)
                    .bind(local, options.backlog)
                    .configureBlocking(false);
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return channels;
    }

    public void unbind()
    {
        assert channels != null;
        for (SelectableChannel channel : channels)
        {
            quietClose(channel);
        }
        channels = null;
    }
}
