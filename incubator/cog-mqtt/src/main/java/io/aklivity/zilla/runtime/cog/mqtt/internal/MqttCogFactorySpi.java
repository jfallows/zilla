/*
 * Copyright 2021-2022 Aklivity Inc.
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
package io.aklivity.zilla.runtime.cog.mqtt.internal;

import io.aklivity.zilla.runtime.engine.cog.CogFactorySpi;
import io.aklivity.zilla.runtime.engine.cog.Configuration;

public final class MqttCogFactorySpi implements CogFactorySpi
{
    @Override
    public String name()
    {
        return MqttCog.NAME;
    }

    @Override
    public MqttCog create(
        Configuration config)
    {
        return new MqttCog(new MqttConfiguration(config));
    }
}
