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

import com.netflix.ice.common.*;
import com.netflix.ice.processor.Instances;
import com.netflix.ice.tag.Product;
import com.netflix.ice.tag.UserTag;

import java.util.Collection;

/**
 * Interface to manager all TagGroupManager and DataManager instances for different products
 */
public interface Managers {

    void init();

    /**
     *
     * @return collection of products
     */
	Collection<Product> getProducts();

	/**
	 *
	 * @return collection of tags
	 */
	Collection<UserTag> getTags();

	/**
	 *
	 * @return array of user tag names
	 */
	String[] getUserTags();

    /**
     *
     * @param product
     * @return TagGroupManager instance for specified product
     */
    TagGroupManager getTagGroupManager(Product product);

    /**
     *
     * @param product
     * @param consolidateType
     * @return cost DataManager instance for specified product and consolidateType
     */
    DataManager getCostManager(Product product, ConsolidateType consolidateType);

    /**
     *
     * @param product
     * @param consolidateType
     * @return usage DataManager instance for specified product and consolidateType
     */
    DataManager getUsageManager(Product product, ConsolidateType consolidateType);

    /**
     * 
     * @param tag
     * @return
     */
    DataManager getTagCoverageManager(UserTag tag);

    /**
     * 
     */
    Instances getInstances();
    
    /**
     * shutdown all manager instances
     */
    void shutdown();
}
