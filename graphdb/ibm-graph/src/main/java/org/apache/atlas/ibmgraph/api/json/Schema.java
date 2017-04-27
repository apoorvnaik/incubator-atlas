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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class Schema {


    private Collection<PropertyKey> propertyKeys = new HashSet<PropertyKey>();
    private List<IndexDefinition> vertexIndexes = new ArrayList<IndexDefinition>();
    private List<EdgeLabel> edgeLabels = new ArrayList<EdgeLabel>();
    private List<IndexDefinition> edgeIndexes = new ArrayList<IndexDefinition>();
    private List<VertexLabel> vertexLabels = new ArrayList<VertexLabel>();


    public static class IndexDefinition {

        private boolean composite;// false for mixed index
        private String name;// name of index
        private Set<String> propertyKeys = new HashSet<String>();
        private boolean unique;

        //transient since not supported in the json
        private transient boolean requiresReindex;

        /**
         * @param indexName
         * @param composite2
         * @param unique2
         * @param indexType
         * @param propertyKeys2
         */
        public IndexDefinition(String indexName, boolean composite, boolean unique, boolean requiresReindex,
                               List<String> propertyKeys) {

            this.name = indexName;
            this.composite = composite;
            this.unique = unique;
            this.requiresReindex = requiresReindex;
            this.propertyKeys.addAll(propertyKeys);

        }

        public IndexDefinition() {

        }

        public boolean requiresReindex() {
            return requiresReindex;
        }

        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public boolean isComposite() {
            return composite;
        }
        public void setComposite(boolean composite) {
            this.composite = composite;
        }
        public boolean isUnique() {
            return unique;
        }
        public void setUnique(boolean unique) {
            this.unique = unique;
        }

        public void addPropertyKey(String name) {
            propertyKeys.add(name);
        }

        public Set<String> getPropertyKeys() {
            return propertyKeys;
        }
        public void setPropertyKeys(Set<String> propertyKeys) {
            this.propertyKeys = propertyKeys;
        }

        @Override
        public String toString() {
            String indexType = composite ? "CompositeIndex" : "MixedIndex";
            return indexType + "[" +
                    "name=" + name +
                    ", unique=" + unique +
                    ", propertyKeys=" + propertyKeys
                    + "]";
        }
    }


    public Schema() {
        super();
    }

    public void addPropertyKey(PropertyKey key) {
        propertyKeys.add(key);

    }

    public void addPropertyKeys(Collection<PropertyKey> keys) {
        propertyKeys.addAll(keys);
    }

    public Collection<PropertyKey> getPropertyKeys() {
        return Collections.unmodifiableCollection(propertyKeys);
    }

    public List<IndexDefinition> getEdgeIndexes() {
        return edgeIndexes;
    }

    public List<IndexDefinition> getVertexIndexes() {
        return vertexIndexes;
    }

    private static class EdgeLabel { }

    private static class VertexLabel { }

    /**
     * @param idx
     */
    public void addEdgeIndex(IndexDefinition idx) {
        edgeIndexes.add(idx);
    }

    /**
     * @param idx
     */
    public void addVertexIndex(IndexDefinition idx) {
        vertexIndexes.add(idx);
    }

    public boolean isEmpty() {
        return propertyKeys.isEmpty() &&
                vertexIndexes.isEmpty() &&
                edgeLabels.isEmpty() &&
                edgeIndexes.isEmpty() &&
                vertexLabels.isEmpty();

    }

    @Override
    public String toString() {
        return "Schema [propertyKeys=" + propertyKeys + ", vertexIndexes=" + vertexIndexes + ", edgeLabels="
                + edgeLabels + ", edgeIndexes=" + edgeIndexes + ", vertexLabels=" + vertexLabels + "]";
    }


}
