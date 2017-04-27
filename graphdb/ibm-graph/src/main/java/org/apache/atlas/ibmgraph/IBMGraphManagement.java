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

package org.apache.atlas.ibmgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.ibmgraph.api.Cardinality;
import org.apache.atlas.ibmgraph.api.GraphDatabaseConfiguration;
import org.apache.atlas.ibmgraph.api.IGraphDatabaseClient;
import org.apache.atlas.ibmgraph.api.IndexStatus;
import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.PropertyDataType;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Schema;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.api.json.Schema.IndexDefinition;
import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;
import org.apache.atlas.ibmgraph.exception.HttpException;
import org.apache.atlas.ibmgraph.http.HttpCode;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.ibmgraph.util.PossibleSuccessCase;
import org.apache.atlas.ibmgraph.util.SchemaSplitter;
import org.apache.atlas.ibmgraph.util.UpdateSchemaRetryStrategy;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This class manages the index creation with graph db. Backing indices which
 * are used by titan to locate the search backend are not configurable by graph
 * db. So backing indices arguments are ignored.
 *
 *
 */
public class IBMGraphManagement implements AtlasGraphManagement {

    private static final int MAX_BATCH_SIZE = GraphDatabaseConfiguration.INSTANCE.getSchemaCreationBatchSize();
    public static final String FAILED_TO_ESTABLISH_A_BACKSIDE_CONNECTION = "Failed to establish a backside connection";
  //we sometimes get this when creating the schema  Example: "with reason java.lang.IllegalStateException: Could not find type for id: 86793."
    public static final String ILLEGAL_STATE = "java.lang.IllegalStateException";

    private static final Logger logger_ = LoggerFactory.getLogger(IBMGraphManagement.class.getName());

    private final IBMGraphMetadata metadata;

    private static final char SEPARATOR_CHAR = 0x1e;
    private static final char[] RESERVED_CHARS = {'{', '}', '"', '$', SEPARATOR_CHAR};

    //we keep track of the indices/property that are needed and create them on demand.
    private final Map<String, Index> pendingIndices_ = new HashMap<>();
    /**
     * Property keys whose creation has been requested but have not been pushed
     * into the graph yet. Pending properties get created implicitly when the
     * index they are used in is created.
     */
    private final Map<String, AtlasPropertyKey> pendingPropertyKeys_ = new HashMap<String, AtlasPropertyKey>();

    //whether this transaction is open (ie it has not been committed or rolled back)
    private boolean isOpen = true;

    private String tenantId;

    private IGraphDatabaseClient client;

    private static final PossibleSuccessCase[] RETRY_CASES = {
            //See comment in UpdateSchemaRetryStrategy.isRetryInternalServerError.  If we get this error,
            //we retry the request.  If the retry results in a BadRequestException, we treat that
            //as a success, since it means that the indices were created successfully and we can't
            //create them again.
            new PossibleSuccessCase(HttpCode.INTERNAL_SERVER_ERROR, IBMGraphManagement.FAILED_TO_ESTABLISH_A_BACKSIDE_CONNECTION),
            new PossibleSuccessCase(HttpCode.BAD_REQUEST, ILLEGAL_STATE)
        };

    public IBMGraphManagement(IBMGraphMetadata metadata, IGraphDatabaseClient client, String tenantId) {
        this.metadata = metadata;
        this.tenantId = tenantId;
        this.client = client;
        GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().addAdditionalBaseKeysAndIndicesIfNeeded(metadata, this);


    }

    @Override
    public boolean containsPropertyKey(String key) {
        return getSchemaProperties().contains(key);
    }

    private Set<String> getSchemaProperties() {

        return Sets.union(metadata.getExistingPropertyKeys().keySet(), getPendingPropertyKeys().keySet());
    }


    @Override
    public void createVertexIndex(String indexName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {
        logger_.info(getLogPrefix() +"Creating mixed index " + indexName + " with backing index " + backingIndex);
        List<AtlasPropertyKey> allKeys = new ArrayList<AtlasPropertyKey>();
        allKeys.addAll(propertyKeys);
        updateKeyList(allKeys);
        buildMixedIndex(indexName, IndexType.VERTEX, allKeys, backingIndex);

    }

    @Override
    public void createEdgeIndex(String indexName, String backingIndex) {
        List<AtlasPropertyKey> propertyKeys = new ArrayList<AtlasPropertyKey>();
        // ibmgraph requires at least one property to be specified in index
        // Make up a random one.  Can't use real property key here since
        //that will confuse the logic in GraphBackedSearchIndexer, which uses
        //the existence of property keys to check whether or not the indices
        //have been created for a particular attribute

        AtlasPropertyKey key = getPropertyKey(indexName);
        if(key == null) {
            key = makePropertyKey(indexName, String.class, AtlasCardinality.SINGLE);
        }
        propertyKeys.add(key);
        //updateKeyList(propertyKeys);
        buildMixedIndex(indexName, IndexType.EDGE, propertyKeys, backingIndex);

    }

    private void buildMixedIndex(String indexName, IndexType indexType, List<AtlasPropertyKey> propertyKeys,
            String backingIndex) {
        boolean unique = false;
        boolean requiresReindex = true;
        boolean isComposite = false;
        List<AtlasPropertyKey> keys = new ArrayList<>(propertyKeys.size() + 1);
        updateKeyList(keys);
        keys.addAll(propertyKeys);
        buildIndex(indexName, indexType, isComposite, unique, requiresReindex, propertyKeys, backingIndex);
    }


    @Override
    public void createFullTextIndex(String indexName, AtlasPropertyKey propertyKey, String backingIndex) {
        List<AtlasPropertyKey> propertyKeys = new ArrayList<AtlasPropertyKey>();
        propertyKeys.add(propertyKey);
        updateKeyList(propertyKeys);
        boolean unique = false;
        boolean requiresReindex = false;
        boolean isComposite = false;
        buildIndex(indexName, IndexType.VERTEX, isComposite, unique, requiresReindex, propertyKeys, null);
    }

    private void updateKeyList(List<AtlasPropertyKey> propertyKeys) {
        propertyKeys.addAll(GraphDatabaseConfiguration.INSTANCE.getTenantGraphStrategy().getAdditionalIndexKeys(this));
    }

    @Override
    public void createExactMatchIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys) {

        List<AtlasPropertyKey> keys = new ArrayList<>(2);
        updateKeyList(keys);
        keys.addAll(propertyKeys);
        createCompositeIndex_(propertyName, keys, isUnique);
    }

    public void createCompositeIndex_(String propertyName, List<AtlasPropertyKey> propertyKeys, boolean isUnique) {

        boolean requiresReindex = true;
        boolean isComposite = true;
        buildIndex(propertyName, IndexType.VERTEX, isComposite, isUnique, requiresReindex,
                propertyKeys, null);
    }

    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, AtlasCardinality cardinality) {
        checkName(propertyName);
        AtlasPropertyKey key = getPropertyKey(propertyName);
        if (key != null) {
            return key;
        }
        PropertyDataType type = GraphDBUtil.createPropertyDataType(propertyClass);
        Cardinality ibmGraphCardinality = GraphDBUtil.createCardinality(cardinality);
        PropertyKey jsonKey = new PropertyKey(propertyName, type, ibmGraphCardinality);
        key = new IBMGraphPropertyKey(jsonKey);
        getPendingPropertyKeys().put(propertyName, key);
        return key;
    }

    @Override
    public AtlasPropertyKey getPropertyKey(String name) {
        AtlasPropertyKey result = metadata.getExistingPropertyKeys().get(name);
        if (result == null) {
            result = getPendingPropertyKeys().get(name);
        }
        return result;
    }

    @Override
    public void addVertexIndexKey(String index, AtlasPropertyKey propertyKey) {

        boolean unique = false;
        boolean requiresReindex = true;
        boolean isComposite = false;
        buildIndex(index, IndexType.VERTEX, isComposite, unique, requiresReindex,
                Collections.singletonList(propertyKey), null);
    }

    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {

        Index index = metadata.getEffectiveIndex(indexName, getPendingIndices());
        if(index == null) {
            return null;
        }
        return new IBMGraphIndex(this, index);
    }

    private static void checkName(String name) {
        //validate property names to be consistent with titan0/titan1
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : RESERVED_CHARS) {
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
        }
    }

    private Map<String, Index> getPendingIndices() {
        return pendingIndices_;
    }
    private Map<String, AtlasPropertyKey> getPendingPropertyKeys() {
        return pendingPropertyKeys_;
    }

    private void buildIndex(String indexName, IndexType indexType, boolean composite, boolean unique,
            boolean reindexRequired, List<AtlasPropertyKey> atlasPropertyKeys, String backingIndex) {

        if (!composite) {
            if (unique) {
                logger_.warn(getLogPrefix()+
                        "WARNING: Trying to create unique mixed index, which is not supported by IBM Graph DB.  Changing unique to false...");
                unique = false;
            }
        }

        Index idx = null;
        if(pendingIndices_.containsKey(indexName)) {
            idx = pendingIndices_.get(indexName);
        }
        else {
            idx = new Index(indexName, composite, unique, reindexRequired, indexType);
            pendingIndices_.put(idx.getName(), idx);
        }
        List<PropertyKey> propertyKeys = Lists.transform(atlasPropertyKeys, AtlasPropertyKeyToPropertyKey.INSTANCE);
        idx.addPropertyKeys(propertyKeys);

        for (AtlasPropertyKey key : atlasPropertyKeys) {
            pendingPropertyKeys_.put(key.getName(), key);
        }

    }

    //delay index creation to accumulate as much as we can.  This
    //is called before we do anything that actually requires the indices
    //to exist.  See IBMGraphDatabaseGraph.applyPendingChanges()
    public void forceCommit() {
        commit();
    }


    @Override
    public void rollback() {
        if(! isOpen) {
            throw new IllegalStateException("Cannot rollback closed graph management transaction.");
        }
        isOpen = false;
       //nothing to do.
    }


    @Override
    public void commit() {

        try {
            if(! isOpen) {
                throw new IllegalStateException("Cannot commit closed graph management transaction.");
            }
            if(! hasChanges()) {
                logger_.debug( getLogPrefix() + " Commit called but nothing to commit.  Returning.");
                return;
            }

            commit_();
            logger_.debug(getLogPrefix() + " updating cache for schema.");
            metadata.applySchemaUpdates(pendingIndices_.values(), pendingPropertyKeys_.values());
        }
        finally {
           isOpen = false;
        }
     }


    private boolean hasChanges() {

        if (pendingIndices_.isEmpty() && pendingPropertyKeys_.isEmpty()) {
            return false;
        }
        return true;
    }


    private void commit_() {

        //The schema updates are divided into batches which are
        //executed sequentially.

        List<Schema> schemaUpdateBatches = createSchemaUpdateBatches(MAX_BATCH_SIZE);

        logger_.debug(getLogPrefix() + " commiting changes to IBM Graph");
        int idx=1;
        for(Schema schemaUpdate : schemaUpdateBatches) {
            logger_.info("Updating schema: Executing batch " + idx + " of " + schemaUpdateBatches.size());
            if(! schemaUpdate.isEmpty()) {
                pushSchemaUpdateToIbmGraph(schemaUpdate);
            }
            idx++;
        }
        waitForIndexAvailibility(pendingIndices_.keySet());


    }

    private List<Schema> createSchemaUpdateBatches(int maxPropertyKeysPerBatch) {

        SchemaSplitter splitter = new SchemaSplitter(MAX_BATCH_SIZE, pendingPropertyKeys_, pendingIndices_, metadata.getExistingPropertyKeys().keySet());
        return splitter.createSchemaUpdateBatches();
    }



    private void pushSchemaUpdateToIbmGraph(Schema s) throws GraphDatabaseException {

        Schema toCreate = s;
        int maxRetries = GraphDatabaseConfiguration.INSTANCE.getCreateSchemaHttpMaxRetries();
        for(int i = 0; i < maxRetries; i++) {
            try {
                if(logger_.isInfoEnabled()) {
                    logger_.info("Making call to IBM Graph to update the schema: try " + (i+1));
                    logger_.info("Schema being created: " + s.toString());
                }
                getClient().updateSchema(toCreate, UpdateSchemaRetryStrategy.INSTANCE);
                return;
            }
            catch(HttpException e) {
                //index commits are atomic on the IBM Graph side.  In some cases, we get an Error response
                //from IBM Graph even though the creation was actually successful.  In these cases, we don't
                //know whether the creation actually succeeded and need to do an additional check to see
                //it they did.  We do this by executing the request again.  If it fails with an 'already exists'
                //exception such as "Composite index Asset.owner already exists. Keys cannot be added to a composite index. Please create a new one.",
                //we know that the original creation was successful and treat it as a success.
                if(isRetryableException(e)) {
                    logger_.warn("Caught retryable exception: " + e.getMessage() + ".  We will retry index creation call.");
                    toCreate = removeSuccessfulSchemaChanges(toCreate);

                    continue;
                }
                throw e;
            }
            catch(GraphDatabaseException e) {
                throw e;
            }
        }
        throw new GraphDatabaseException("Could not update schema: exceeded retry count.");
    }

    /**
     * Creates a new Schema object with the index changes that
     * were successfully applied remove.  The property creations
     * are duplicated in the new request.
     *
     * @param originalRequest the original schema request
     * @return
     */
    private Schema removeSuccessfulSchemaChanges(Schema originalRequest) {
        logger_.info("Determining what indices were created...");
        Schema updatedSchema = new Schema();
        //Leave the property key requests in the update schema request.  If the property
        //keys exist, they will be ignored by IBM Graph.
        updatedSchema.addPropertyKeys(originalRequest.getPropertyKeys());
        for(IndexDefinition def : originalRequest.getEdgeIndexes()) {
            IndexDefinition updated = removeExistingPropertyKeys(def);
            if(updated != null) {
                logger_.info("Adding edge index "+ def.getName() + " to indices to update.");
                updatedSchema.addEdgeIndex(updated);
            }
        }
        for(IndexDefinition def : originalRequest.getVertexIndexes()) {
            IndexDefinition updated = removeExistingPropertyKeys(def);
            if(updated != null) {
                logger_.info("Adding vertex index "+ def.getName() + " to indices to update.");
                updatedSchema.addVertexIndex(updated);
            }
        }
        return updatedSchema;
    }

    private IndexDefinition removeExistingPropertyKeys(IndexDefinition idx) {
        Collection<String> existingPropertyKeys = client.getPropertyKeysInIndex(idx.getName());

        if(idx.isComposite()) {
            //Just check if it exists.  Only mixed indices can be updated.  Also, the composite index
            //status does not indicate the status of the individual property keys that
            //make up the index.
            if(existingPropertyKeys.isEmpty()) {
                //not created
                return idx;
            }
            else {
                return null;
            }
        }
        else {
            //mixed index

            IndexDefinition result = new IndexDefinition();
            result.setComposite(false);
            result.setName(idx.getName());
            result.setUnique(idx.isUnique());

            for(String key : idx.getPropertyKeys()) {
                if(! existingPropertyKeys.contains(key)) {
                    result.addPropertyKey(key);
                }
            }
            if(result.getPropertyKeys().isEmpty()) {
                return null;
            }
            return result;
        }

    }


    private boolean isRetryableException(HttpException e) {
        for(PossibleSuccessCase caseToCheck : RETRY_CASES)  {
            if(caseToCheck.matches(e)) {
                //retry
                return true;
            }
        }
        return false;
    }

    private void waitForIndexAvailibility(Collection<String> indexNames) {

        logger_.info(getLogPrefix()+" Waiting for new indices to become ENABLED enabled.");
        long start = System.currentTimeMillis();
        long timeoutTime = start + 1000 * 60* 60 * 2; // wait at most 2 hours

        Collection<String> toActivate = new HashSet<String>();
        toActivate.addAll(indexNames);

        //keeps track of what indices are still not enabled and their
        //current status
        Map<String,IndexStatus> remainingIndexStates = new HashMap<>();

        //initialize states
        for(String name : indexNames) {
            remainingIndexStates.put(name, IndexStatus.NOT_PRESENT);
        }

        // wait for index to become active
        long activationTime = start + 10*60*1000; //activate indices after waiting for 10 minutes
        while (System.currentTimeMillis() < timeoutTime) {

            updateIndexStatus(remainingIndexStates);

            Iterator<Map.Entry<String,IndexStatus>> it = remainingIndexStates.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<String, IndexStatus> statusEntry = it.next();
                IndexStatus status = statusEntry.getValue();
                String indexName = statusEntry.getKey();

                if(status == IndexStatus.ENABLED) {
                    toActivate.remove(indexName);
                    it.remove();

                }

                //run script if indexes have not become enabled after 10 minutes
                if(System.currentTimeMillis() > activationTime && toActivate.contains(indexName) && status == IndexStatus.REGISTERED) {
                    Index idx = lookupPendingIndex(indexName);
                    boolean activated = getClient().activateIndex(indexName, idx.getPropertyKeyNames());
                    if(activated) {
                        toActivate.remove(indexName);
                    }
                }
            }

            if (remainingIndexStates.size() == 0) {
                long completeTime = System.currentTimeMillis();
                logger_.info(getLogPrefix()+" Indices enabled after " + (GraphDBUtil.formatTime(completeTime - start)));
                return;
            }

            logActivationStatus(remainingIndexStates);
            logger_.info(getLogPrefix()+ "Elapsed time so far: " + GraphDBUtil.formatTime((System.currentTimeMillis() - start)));

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        logger_.error(getLogPrefix() + "Timed out waiting for indices to be enabled.");

    }


    private void updateIndexStatus(Map<String, IndexStatus> remainingIndexStates) {
        for(String key : remainingIndexStates.keySet()) {
            Index idx = lookupPendingIndex(key);
            List<String> expectedKeyNames = new ArrayList<>();
            if(idx.isComposite()) {
                expectedKeyNames.add(idx.getName());
            }
            else {
                expectedKeyNames.addAll(idx.getPropertyKeyNames());
            }
            IndexStatus status = getClient().getOverallStatus(key, expectedKeyNames);
            remainingIndexStates.put(key, status);
        }

    }

    private void logActivationStatus(Map<String,IndexStatus> remainingIndices) {
        logger_.info(getLogPrefix() +"Waiting for indices to be enabled...");

        if (logger_.isInfoEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Waiting for the following indices: ");

            List<String> sortedIndexNames = new ArrayList<String>();
            sortedIndexNames.addAll(remainingIndices.keySet());
            Collections.sort(sortedIndexNames);
            Iterator<String> it = sortedIndexNames.iterator();
            while (it.hasNext()) {
                String key = it.next();
                builder.append(key);
                builder.append("[");
                builder.append(remainingIndices.get(key));
                builder.append("]");
                if (it.hasNext()) {
                    builder.append(", ");
                }
            }
            logger_.info(getLogPrefix()+ builder.toString());
        }
    }

    private Index lookupPendingIndex(String name) {
        if (pendingIndices_.containsKey(name)) {
            Index def = pendingIndices_.get(name);
            return def;
        }
        return null;
    }

    @Override
    public String toString() {
        return "IBMGraphManagement[graph=" + metadata.getGraphName()+ ", tenant=" + tenantId + "]";
    }

    private String getLogPrefix() {
        return "[" + metadata.getGraphName() + ":" + tenantId + "]: ";
    }


    private IGraphDatabaseClient getClient() {
        return client;
    }



}
