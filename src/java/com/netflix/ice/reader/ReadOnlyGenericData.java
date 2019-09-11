/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.ice.reader;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;

public abstract class ReadOnlyGenericData<D> {
    D[][] data;
    private Collection<TagGroup> tagGroups;

    public ReadOnlyGenericData(D[][] data, Collection<TagGroup> tagGroups) {
        this.data = data;
        this.tagGroups = tagGroups;
    }

    public D[] getData(int i) {
        return data[i];
    }

    public int getNum() {
        return data.length;
    }

    public Collection<TagGroup> getTagGroups() {
        return tagGroups;
    }
    
    abstract protected D[][] newDataMatrix(int size);
    abstract protected D[] newDataArray(int size);
    abstract protected D readValue(DataInput in) throws IOException ;

    public void deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException {

        int numKeys = in.readInt();
        List<TagGroup> keys = Lists.newArrayList();
        for (int j = 0; j < numKeys; j++) {
            keys.add(TagGroup.Serializer.deserialize(accountService, productService, in));
        }

        int num = in.readInt();
        D[][] data = newDataMatrix(num);
        for (int i = 0; i < num; i++)  {
            data[i] = newDataArray(keys.size());
            boolean hasData = in.readBoolean();
            if (hasData) {
                for (int j = 0; j < keys.size(); j++) {
                    D v = readValue(in);
                    if (v != null) {
                        data[i][j] = v;
                    }
                }
            }
        }

        this.data = data;
        this.tagGroups = keys;
    }
}
