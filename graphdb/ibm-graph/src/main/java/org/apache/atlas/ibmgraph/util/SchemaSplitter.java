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
package org.apache.atlas.ibmgraph.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.api.json.Schema.IndexDefinition;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

/**
 *
 */
public class SchemaSplitter {

    private int maxBatchSize;
    private Map<String, AtlasPropertyKey> pendingPropertyKeys_;
    private Map<String, Index> pendingIndices_;
    private Set<String> existingPropertyKeys_;



    public SchemaSplitter(int maxBatchSize, Map<String, AtlasPropertyKey> pendingPropertyKeys,
            Map<String, Index> pendingIndices, Set<String> existingPropertyKeys) {
        super();
        this.maxBatchSize = maxBatchSize;
        pendingPropertyKeys_ = pendingPropertyKeys;
        pendingIndices_ = pendingIndices;
        existingPropertyKeys_ = existingPropertyKeys;
    }

    /**
     * Creates batches of schema calls that each create at most maxBatchSize
     * property keys in each call.  All of the index creation/updates are
     * guaranteed to be applied.
     *
     *
     */
    public List<Schema> createSchemaUpdateBatches() {

        //Create batches by splitting up the property keys for the update
        //into batches.  For each batch, we construct a Schema that
        //contains those property keys.  We include the composite indices
        //that use only the property keys defined in a given batch (or that
        //were previously defined) and mixed indices that use those property
        //keys, restricted to only adding property keys that are in the
        //batch.  Existing property keys being added to a mixed index
        //are all included in the first batch.

        Map<String, Set<Index>> pendingIndiceByPropertyKey = getIndicesByPropertyKey();
        List<String> allKeys = new ArrayList<>(pendingIndiceByPropertyKey.keySet());

        //sort keys for consistency in schema creation batches
        Collections.sort(allKeys);

        Batcher<String> batcher = new Batcher<>(allKeys, maxBatchSize);
        List<Schema> schemas = new ArrayList<>(batcher.getBatchCount());

        //Existing property keys, including those created in the current and
        //previous batches.
        Collection<String> cumulativeExistingPropertyKeys = new HashSet<String>();
        cumulativeExistingPropertyKeys.addAll(existingPropertyKeys_);

        List<Index> indicesWithOnlyExistingPropertyKeys = getIndicesWithOnlyExistingPropertyKey();

        int counter = 0;
        do {
            counter++;
            boolean isFirstBatch = (counter == 1);

            Schema currentSchema = new Schema();
            schemas.add(currentSchema);

            Set<Index> indicesToProcess = new HashSet<>();

            //New property keys that are being created as part of this batch
            List<String> keysForBatch = null;

            if(batcher.hasNext()) {
                keysForBatch = batcher.next();
                cumulativeExistingPropertyKeys.addAll(keysForBatch);
                for(String keyName : keysForBatch) {
                    indicesToProcess.addAll(pendingIndiceByPropertyKey.get(keyName));
                    AtlasPropertyKey key = pendingPropertyKeys_.get(keyName);
                    currentSchema.addPropertyKey(GraphDBUtil.unwrapPropertyKey(key));
                }
            }
            else {
                keysForBatch = Collections.emptyList();
            }

            Collection<String> allowedMixedIndexKeys;

            if(isFirstBatch) {
                indicesToProcess.addAll(indicesWithOnlyExistingPropertyKeys);
                //include the property keys that already exist
                //in the first batch only.  We only want to add these
                //to the mixed index once.  After adding those, we
                //only add the new property keys.
                allowedMixedIndexKeys = cumulativeExistingPropertyKeys;
            }
            else {
                allowedMixedIndexKeys = keysForBatch;
            }

            for(Index idx : indicesToProcess) {

                Index filteredIndex = idx;

                if(idx.isComposite()) {

                    if(! idx.onlyUses(cumulativeExistingPropertyKeys)) {
                        //not all property keys this index needs have been added.  Wait
                        //until they are all present.
                        continue;
                    }
                }
                else {
                    filteredIndex = idx.filter(allowedMixedIndexKeys);
                }

                if(filteredIndex.getPropertyKeys().size() != 0) {

                    IndexDefinition indexDef =  GraphDBUtil.convertIndex(filteredIndex);

                    if (filteredIndex.getType() == IndexType.VERTEX) {
                        currentSchema.addVertexIndex(indexDef);
                    } else {
                        currentSchema.addEdgeIndex(indexDef);
                    }
                }
            }


        } while(batcher.hasNext());

        return schemas;
    }

    private List<Index> getIndicesWithOnlyExistingPropertyKey() {
        List<Index> result = new ArrayList<>();

        for(Index index : pendingIndices_.values()) {
            //not new property key
            boolean hasOnlyExistingPropertKeys = true;

            for(PropertyKey key : index.getPropertyKeys()) {
                if( isNewPropertyKey(key)) {
                    hasOnlyExistingPropertKeys = false;
                    break;
                }
            }
            if(hasOnlyExistingPropertKeys) {
                result.add(index);
            }
        }
        return result;

    }

    private Map<String, Set<Index>> getIndicesByPropertyKey() {
        Map<String,Set<Index>> pendingIndiceByPropertyKey = new HashMap<>();

        for(Index index : pendingIndices_.values()) {
            for(PropertyKey key : index.getPropertyKeys()) {
                if(isNewPropertyKey(key)) {
                    Set<Index> indices = pendingIndiceByPropertyKey.get(key.getName());
                    if(indices == null) {
                        indices = new HashSet<Index>();
                        pendingIndiceByPropertyKey.put(key.getName(), indices);
                    }
                    indices.add(index);
                }
            }
        }
        //add new property keys that are not associated with any indices
        for(String name : pendingPropertyKeys_.keySet()) {
            if(! pendingIndiceByPropertyKey.containsKey(name)) {
                if(! existingPropertyKeys_.contains(name)) {
                    pendingIndiceByPropertyKey.put(name, Collections.emptySet());
                }
            }
        }
        return pendingIndiceByPropertyKey;
    }

    private boolean isNewPropertyKey(PropertyKey key) {
        return ! existingPropertyKeys_.contains(key.getName());
    }


}
