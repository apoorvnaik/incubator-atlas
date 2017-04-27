/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.ibmgraph.api.json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * This class defines the index (vertex or edge index)
 *
 *
 */
public class Index {
    /**
     * index name
     */
    private final String name;// name of index
    private final boolean composite;// false for mixed index
    private final boolean unique;
    private final IndexType type;
    //transient - not supported in json
    private transient boolean requiresReindex;

    public static enum IndexType {
        VERTEX,
        EDGE;
    }

    private final List<PropertyKey> propertyKeys = Collections.synchronizedList(new ArrayList<PropertyKey>());

    public Index(String name, boolean composite, boolean unique,
            boolean requiresReindex,
            IndexType type,
            List<PropertyKey> propertyKeys) {
        super();
        this.name = name;
        this.composite = composite;
        this.unique = unique;
        this.type = type;
        this.requiresReindex = requiresReindex;
        this.propertyKeys.addAll(propertyKeys);
    }

    public Index(String name, boolean composite, boolean unique, boolean requiresReindex,
            IndexType type) {
        super();
        this.name = name;
        this.composite = composite;
        this.unique = unique;
        this.requiresReindex = requiresReindex;
        this.type = type;
    }

    public boolean isComposite() {
        return composite;
    }

    public String getName() {
        return name;
    }


    public boolean isUnique() {
        return unique;
    }


    public List<PropertyKey> getPropertyKeys() {
        return propertyKeys;
    }

    public List<String> getPropertyKeyNames() {

        return Lists.transform(propertyKeys, new Function<PropertyKey,String>() {

            @Override
            public String apply(PropertyKey input) {
                return input.getName();
            }

        });
    }


    public void addPropertyKey(PropertyKey key) {
        propertyKeys.add(key);
    }

    public IndexType getType() {
        return type;
    }


    public void addPropertyKeys(List<PropertyKey> toAdd) {
        propertyKeys.addAll(toAdd);

    }

    public boolean requiresReindex() {
        return requiresReindex;
    }

    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37*result + getName().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if(!(other instanceof Index)) {
            return false;
        }
        Index otherIdx = (Index)other;
        return otherIdx.getName().equals(getName());
    }

    /**
     * Creates an index with a subset of the index keys
     */
    public Index filter(Collection<String> allowedKeys) {
        Index idx = new Index(name, composite, unique, requiresReindex, type);
        for(PropertyKey key : getPropertyKeys()) {
            if(allowedKeys.contains(key.getName())) {
                idx.addPropertyKey(key);
            }
        }
        return idx;
    }

    public boolean onlyUses(Collection<String> allowedKeys) {
        for(PropertyKey key : getPropertyKeys()) {
            if(! allowedKeys.contains(key.getName())) {
               return false;
            }
        }
        return true;
    }
}
