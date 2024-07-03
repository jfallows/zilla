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
package io.aklivity.zilla.runtime.catalog.filesystem.internal;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.aklivity.zilla.runtime.engine.Configuration;
import io.aklivity.zilla.runtime.engine.catalog.Catalog;
import io.aklivity.zilla.runtime.engine.catalog.CatalogFactory;

public class FilesystemCatalogFactoryTest
{
    @Test
    public void shouldLoadAndCreate() throws Exception
    {
        Configuration config = new Configuration();
        CatalogFactory factory = CatalogFactory.instantiate();
        Catalog catalog = factory.create(FilesystemCatalog.NAME, config);

        assertThat(catalog, instanceOf(FilesystemCatalog.class));
        assertEquals(FilesystemCatalog.NAME, catalog.name());
    }
}
