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

import com.google.common.collect.Lists;
import com.netflix.ice.common.TagGroup;

import java.io.DataInput;
import java.io.IOException;

public class ReadOnlyData extends ReadOnlyGenericData<Double> {
    public ReadOnlyData() {
        super(new Double[][]{}, Lists.<TagGroup>newArrayList());
    }

	@Override
	protected Double[][] newDataMatrix(int size) {
		return new Double[size][];
	}

	@Override
	protected Double[] newDataArray(int size) {
		return new Double[size];
	}

	@Override
	protected Double readValue(DataInput in) throws IOException {
		Double v = in.readDouble();
		return v == null || v == 0 ? null : v;
	}
}
