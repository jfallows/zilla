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
package io.aklivity.zilla.runtime.engine.test.internal.model.config;

import java.util.List;
import java.util.function.Function;

import io.aklivity.zilla.runtime.engine.config.CatalogedConfig;
import io.aklivity.zilla.runtime.engine.config.ModelConfig;

public class TestModelConfig extends ModelConfig
{
    public final int length;
    public final boolean read;
    public final String keyRef;
    public final String mode;

    public transient long vaultId;

    public TestModelConfig(
        int length,
        List<CatalogedConfig> cataloged,
        boolean read,
        String keyRef,
        String mode)
    {
        super("test", cataloged);
        this.length = length;
        this.read = read;
        this.keyRef = keyRef;
        this.mode = mode;
    }

    public static <T> TestModelConfigBuilder<T> builder(
        Function<ModelConfig, T> mapper)
    {
        return new TestModelConfigBuilder<>(mapper);
    }

    public static TestModelConfigBuilder<TestModelConfig> builder()
    {
        return new TestModelConfigBuilder<>(TestModelConfig.class::cast);
    }
}
