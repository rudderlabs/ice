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
package com.netflix.ice.processor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.ice.common.AccountService;
import com.netflix.ice.common.ProductService;
import com.netflix.ice.common.TagGroup;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReadWriteData is a list of maps that hold usage and cost data for AWS products.
 * The list index is the hour in the month for the instance data.
 * The map keys are a TagGroup which is the unique combination of Tags associated with
 * the cost or usage number stored as the value in the map.
 */
public class ReadWriteData {
    private List<Map<TagGroup, Double>> data;

    public ReadWriteData() {
        data = Lists.newArrayList();
    }

    private ReadWriteData(List<Map<TagGroup, Double>> data) {
        this.data = data;
    }

    public int getNum() {
        return data.size();
    }

    void cutData(int num) {
        if (data.size() > num)
            data = data.subList(0, num);
    }

    public Map<TagGroup, Double> getData(int i) {
        return getCreateData(data, i);
    }

    void setData(List<Map<TagGroup, Double>> newData, int startIndex, boolean merge) {
        for (int i = 0; i < newData.size(); i++) {
            int index = startIndex + i;

            if (index > data.size()) {
                getCreateData(data, index-1);
            }
            if (index >= data.size()) {
                data.add(newData.get(i));
            }
            else {
                if (merge) {
                    Map<TagGroup, Double> existed = data.get(index);
                    for (Map.Entry<TagGroup, Double> entry: newData.get(i).entrySet()) {
                        existed.put(entry.getKey(), entry.getValue());
                    }
                }
                else {
                    data.set(index, newData.get(i));
                }
            }
        }
    }
    
    void putAll(ReadWriteData data) {
    	setData(data.data, 0, true);
    }

    static Map<TagGroup, Double> getCreateData(List<Map<TagGroup, Double>> data, int i) {
        if (i >= data.size()) {
            for (int j = data.size(); j <= i; j++) {
                data.add(Maps.<TagGroup, Double>newHashMap());
            }
        }
        return data.get(i);
    }

    public Collection<TagGroup> getTagGroups() {
        Set<TagGroup> keys = Sets.newTreeSet();

        for (Map<TagGroup, Double> map: data) {
            keys.addAll(map.keySet());
        }

        return keys;
    }

    public static class Serializer {
        protected static Logger logger = LoggerFactory.getLogger(Serializer.class);
        public static void serialize(DataOutput out, ReadWriteData data) throws IOException {

            Collection<TagGroup> keys = data.getTagGroups();
            out.writeInt(keys.size());
            for (TagGroup tagGroup: keys) {
                TagGroup.Serializer.serialize(out, tagGroup);
            }

            out.writeInt(data.data.size());
            for (int i = 0; i < data.data.size(); i++) {
                Map<TagGroup, Double> map = data.getData(i);
                out.writeBoolean(map.size() > 0);
                if (map.size() > 0) {
                    for (TagGroup tagGroup: keys) {
                        Double v = map.get(tagGroup);
                        out.writeDouble(v == null ? 0 : v);
                    }
                }
            }
        }

        public static ReadWriteData deserialize(AccountService accountService, ProductService productService, DataInput in) throws IOException {

            int numKeys = in.readInt();
            List<TagGroup> keys = Lists.newArrayList();
            for (int j = 0; j < numKeys; j++) {
                keys.add(TagGroup.Serializer.deserialize(accountService, productService, in));
            }

            List<Map<TagGroup, Double>> data = Lists.newArrayList();
            int num = in.readInt();
            for (int i = 0; i < num; i++)  {
                Map<TagGroup, Double> map = Maps.newHashMap();
                boolean hasData = in.readBoolean();
                if (hasData) {
                    for (int j = 0; j < keys.size(); j++) {
                        double v = in.readDouble();
                        if (v != 0) {
                            map.put(keys.get(j), v);
                        }
                    }
                }
                data.add(map);
            }

            return new ReadWriteData(data);
        }
        
        public static void serializeCsv(OutputStreamWriter out, ReadWriteData data) throws IOException {
        	// write the header
        	out.write("index,");
        	TagGroup.Serializer.serializeCsvHeader(out);
        	out.write(",data\n");
            for (int i = 0; i < data.data.size(); i++) {
                Map<TagGroup, Double> map = data.getData(i);
                for (Entry<TagGroup, Double> entry: map.entrySet()) {
                	out.write("" + i + ",");
                	TagGroup.Serializer.serializeCsv(out, entry.getKey());
                    out.write(",");
                    Double v = entry.getValue();
                    if (v != null)
                    	out.write(v.toString());
                    out.write("\n");
                }
            }
        }

        public static ReadWriteData deserializeCsv(AccountService accountService, ProductService productService, BufferedReader in) throws IOException {
            List<Map<TagGroup, Double>> data = Lists.newArrayList();
            
            String line;
            
            // skip the header
            in.readLine();

            Map<TagGroup, Double> map = null;
            while ((line = in.readLine()) != null) {
            	String[] items = line.split(",");
            	int hour = Integer.parseInt(items[0]);
            	while (hour >= data.size()) {
            		map = Maps.newHashMap();
            		data.add(map);
            	}
            	map = data.get(hour);
            	TagGroup tag = TagGroup.getTagGroup(items[1], items[2], items[3], items[4], items[5], items[6], items[7], items[8], accountService, productService);
            	Double v = Double.parseDouble(items[9]);
            	map.put(tag, v);
            }

            return new ReadWriteData(data);
        }
    }
}
