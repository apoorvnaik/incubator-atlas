/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.discovery;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.util.SearchTracker;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.discovery.SearchPipeline.Util.isReferredEntityAttr;

@Component
public class SearchPipeline {
    private static final Logger LOG      = LoggerFactory.getLogger(SearchPipeline.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("SearchPipeline");

    enum ExecutionMode { SOLR, GREMLIN, MIXED }

    enum IndexResultType { TAG, ENTITY, TEXT }

    private final SolrStep                  solrStep;
    private final AtlasGraphStep            atlasGraphStep;
    private final GremlinStep               gremlinStep;
    private final SearchTracker             searchTracker;
    private final Configuration             atlasConfiguration;

    @Inject
    public SearchPipeline(SolrStep solrStep, AtlasGraphStep atlasGraphStep, GremlinStep gremlinStep,
                          SearchTracker searchTracker, Configuration atlasConfiguration) {
        this.solrStep           = solrStep;
        this.atlasGraphStep     = atlasGraphStep;
        this.gremlinStep        = gremlinStep;
        this.searchTracker      = searchTracker;
        this.atlasConfiguration = atlasConfiguration;
    }

    public List<AtlasVertex> run(PipelineContext context) throws AtlasBaseException {
        final List<AtlasVertex> ret;

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "SearchPipeline.run("+ context +")");
        }

        String searchId   = searchTracker.add(context); // For future cancellation

        try {
            ExecutionMode mode = determineExecutionMode(context);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Execution mode {}", mode);
            }

            switch (mode) {
                case SOLR:
                    ret = runOnlySolr(context);
                    break;

                case GREMLIN:
                    ret = runOnlyGremlin(context);
                    break;

                case MIXED:
                    ret = runMixed(context);
                    break;

                default:
                    ret = Collections.emptyList();
            }
        } finally {
            searchTracker.remove(searchId);

            AtlasPerfTracer.log(perf);
        }

        return ret;
    }

    private List<AtlasVertex> runOnlySolr(PipelineContext context) throws AtlasBaseException {
        // Only when there's no tag and query
        List<AtlasVertex> results = new ArrayList<>();

        while (results.size() < context.getSearchParameters().getLimit()) {
            if (context.getForceTerminate()) {
                LOG.debug("search has been terminated");

                break;
            }

            // Execute solr search only
            solrStep.execute(context);

            List<AtlasVertex> stepResults = getIndexResults(context);

            context.incrementSearchRound();

            addToResult(results, stepResults, context.getSearchParameters().getLimit());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Pipeline iteration {}: stepResults={}; totalResult={}", context.getIterationCount(), stepResults.size(), results.size());
            }

            if (CollectionUtils.isEmpty(stepResults)) {
                // If no result is found any subsequent iteration then just stop querying the index
                break;
            }
        }

        if (context.getIndexResultType() == IndexResultType.TAG) {
            List<AtlasVertex> entityVertices = new ArrayList<>(results.size());

            for (AtlasVertex tagVertex : results) {
                Iterable<AtlasEdge> edges = tagVertex.getEdges(AtlasEdgeDirection.IN);

                for (AtlasEdge edge : edges) {
                    AtlasVertex entityVertex = edge.getOutVertex();

                    entityVertices.add(entityVertex);
                }
            }

            results = entityVertices;
        }

        return results;
    }

    private List<AtlasVertex> runOnlyGremlin(PipelineContext context) throws AtlasBaseException {
        List<AtlasVertex> results = new ArrayList<>();

        while (results.size() < context.getSearchParameters().getLimit()) {
            if (context.getForceTerminate()) {
                LOG.debug("search has been terminated");

                break;
            }

            if (RandomUtils.nextDouble(0.0, 1.0) < 0.5) {
                LOG.info("Using AtlasGraphStep");
                atlasGraphStep.execute(context);
            } else {
                LOG.info("Using GremlinStep");
                gremlinStep.execute(context);
            }

            List<AtlasVertex> stepResults = getGremlinResults(context);

            context.incrementSearchRound();

            addToResult(results, stepResults, context.getSearchParameters().getLimit());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Pipeline iteration {}: stepResults={}; totalResult={}", context.getIterationCount(), stepResults.size(), results.size());
            }

            if (CollectionUtils.isEmpty(stepResults)) {
                // If no result is found any subsequent iteration then just stop querying the index
                break;
            }
        }

        return results;
    }

    /*
        1. Index processes few attributes and then gremlin processes rest
            1.1 Iterate for gremlin till the index results are non null
        2. Index processes all attributes, gremlin has nothing to do

        Sometimes the result set might be less than the max limit and we need to iterate until the result set is full
        or the iteration doesn't return any results

     */
    private List<AtlasVertex> runMixed(PipelineContext context) throws AtlasBaseException {
        List<AtlasVertex> results = new ArrayList<>();

        while (results.size() < context.getSearchParameters().getLimit()) {
            if (context.getForceTerminate()) {
                LOG.debug("search has been terminated");

                break;
            }

            // Execute Solr search and then pass it to the Gremlin step (if needed)
            solrStep.execute(context);

            if (!context.hasIndexResults()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No index results in iteration {}", context.getIterationCount());
                }

                // If no result is found any subsequent iteration then just stop querying the index
                break;
            }

            // Attributes partially processed by Solr, use gremlin to process remaining attribute(s)
            atlasGraphStep.execute(context);

            context.incrementSearchRound();

            List<AtlasVertex> stepResults = getGremlinResults(context);

            addToResult(results, stepResults, context.getSearchParameters().getLimit());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Pipeline iteration {}: stepResults={}; totalResult={}", context.getIterationCount(), stepResults.size(), results.size());
            }
        }

        return results;
    }

    private void addToResult(List<AtlasVertex> result, List<AtlasVertex> stepResult, int maxLimit) {
        if (result != null && stepResult != null && result.size() < maxLimit) {
            for (AtlasVertex vertex : stepResult) {
                result.add(vertex);

                if (result.size() >= maxLimit) {
                    break;
                }
            }
        }
    }

    private List<AtlasVertex> getIndexResults(PipelineContext pipelineContext) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (pipelineContext.hasIndexResults()) {
            Iterator<AtlasIndexQuery.Result> iter = pipelineContext.getIndexResultsIterator();

            while(iter.hasNext()) {
                ret.add(iter.next().getVertex());
            }
        }

        return ret;
    }

    private List<AtlasVertex> getGremlinResults(PipelineContext pipelineContext) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (pipelineContext.hasGremlinResults()) {
            Iterator<AtlasVertex> iter = pipelineContext.getGremlinResultIterator();

            while (iter.hasNext()) {
                ret.add(iter.next());
            }
        }

        return ret;
    }

    private ExecutionMode determineExecutionMode(PipelineContext context) {
        SearchParameters        searchParameters   = context.getSearchParameters();
        AtlasClassificationType classificationType = context.getClassificationType();
        AtlasEntityType         entityType         = context.getEntityType();
        int                     solrCount          = 0;
        int                     gremlinCount       = 0;

        if (StringUtils.isNotEmpty(searchParameters.getQuery())) {
            solrCount++;

            // __state index only exists in vertex_index
            if (searchParameters.getExcludeDeletedEntities()) {
                gremlinCount++;
            }
        }

        if (classificationType != null) {
            Set<String> typeAndAllSubTypes = classificationType.getTypeAndAllSubTypes();

            if (typeAndAllSubTypes.size() > atlasConfiguration.getInt(Constants.INDEX_SEARCH_MAX_TAGS_COUNT, 10)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Classification type {} has too many subTypes ({}) to use in Solr search. Gremlin will be used to execute the search",
                              classificationType.getTypeName(), typeAndAllSubTypes.size());
                }

                gremlinCount++;
            } else {
                if (hasNonIndexedAttrViolation(classificationType, context, searchParameters.getTagFilters())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Tag filters not suitable for Solr search. Gremlin will be used to execute the search");
                    }

                    gremlinCount++;
                } else {
                    solrCount++;

                    // __state index only exist in vertex_index
                    if (searchParameters.getExcludeDeletedEntities()) {
                        gremlinCount++;
                    }
                }
            }
        }

        if (entityType != null) {
            Set<String> typeAndAllSubTypes = entityType.getTypeAndAllSubTypes();

            if (typeAndAllSubTypes.size() > atlasConfiguration.getInt(Constants.INDEX_SEARCH_MAX_TYPES_COUNT, 10)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Entity type {} has too many subTypes ({}) to use in Solr search. Gremlin will be used to execute the search",
                              entityType.getTypeName(), typeAndAllSubTypes.size());
                }

                gremlinCount++;
            } else {
                if (hasNonIndexedAttrViolation(entityType, context, searchParameters.getEntityFilters())) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Entity filters not suitable for Solr search. Gremlin will be used to execute the search");
                    }

                    gremlinCount++;
                } else {
                    solrCount++;
                }
            }
        }

        ExecutionMode mode = ExecutionMode.MIXED;

        if (solrCount == 1 && gremlinCount == 0) {
            mode = ExecutionMode.SOLR;
        } else if (gremlinCount == 1 && solrCount == 0) {
            mode = ExecutionMode.GREMLIN;
        }

        return mode;
    }

    // If Index can't process all attributes and any of the non-indexed attribute is present in OR nested within AND
    // then the only way is Gremlin
    // A violation (here) is defined as presence of non-indexed attribute within any OR clause nested under an AND clause
    // the reason being that the index would not be able to process the nested OR attribute which might result in
    // exclusion of valid result (vertex)
    private boolean hasNonIndexedAttrViolation(AtlasStructType structType, PipelineContext context, FilterCriteria filterCriteria) {
        return hasNonIndexedAttrViolation(structType, context, filterCriteria, false);
    }

    private boolean hasNonIndexedAttrViolation(AtlasStructType structType, PipelineContext context, FilterCriteria filterCriteria, boolean enclosedInOrCondition) {
        if (filterCriteria == null) {
            return false;
        }

        boolean ret             = false;
        Condition filterCondition = filterCriteria.getCondition();
        List<FilterCriteria> criterion       = filterCriteria.getCriterion();

        if (filterCondition != null && CollectionUtils.isNotEmpty(criterion)) {
            if (!enclosedInOrCondition) {
                enclosedInOrCondition = filterCondition == Condition.OR;
            }

            // If we have nested criterion let's find any nested ORs with non-indexed attr
            for (FilterCriteria criteria : criterion) {
                ret |= hasNonIndexedAttrViolation(structType, context, criteria, enclosedInOrCondition);

                if (ret) {
                    break;
                }
            }
        } else if (StringUtils.isNotEmpty(filterCriteria.getAttributeName())) {
            String attributeName = filterCriteria.getAttributeName();
            if (isReferredEntityAttr(structType, attributeName)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring the referred entity attribute");
                }
                String[] attrParts = attributeName.split("\\.");
                // TODO: Throw exception in case there is multi-level attribute reference
                // Gremlin step can use this to make further decisions
                context.addReferredEntityAttr(attrParts[0], attrParts[1], attributeName);
                ret = true;
            } else {
                // If attribute qualified name doesn't exist in the vertex index we potentially might have a problem
                try {
                    String qualifiedAttributeName = structType.getQualifiedAttributeName(attributeName);

                    Set<String> indexedKeys = context.getIndexedKeys();
                    ret = CollectionUtils.isEmpty(indexedKeys) || !indexedKeys.contains(qualifiedAttributeName);

                    if (ret) {
                        LOG.warn("search includes non-indexed attribute '{}'; might cause poor performance", qualifiedAttributeName);
                    }
                } catch (AtlasBaseException e) {
                    LOG.warn(e.getMessage());

                    ret = true;
                }
            }
        }

        // return ret && enclosedInOrCondition;

        return ret;
    }

    public interface PipelineStep {
        void execute(PipelineContext context) throws AtlasBaseException;
    }

    public static class PipelineContext {

        private final SearchParameters        searchParameters;
        private final AtlasEntityType         entityType;
        private final AtlasClassificationType classificationType;
        private final Set<String>             indexedKeys;

        private int     iterationCount;
        private boolean forceTerminate;
        private int     currentOffset;
        private int     maxLimit;

        // Continuous processing stuff
        private Set<String> tagSearchAttributes                             = new HashSet<>();
        private Set<String> entitySearchAttributes                          = new HashSet<>();

        private ReferredEntityContext referredEntityContext = new ReferredEntityContext();

        // Solr
        private Set<String> tagAttrProcessedBySolr    = new HashSet<>();
        private Set<String> entityAttrProcessedBySolr = new HashSet<>();

        // Results related stuff
        private IndexResultType                  indexResultType;
        private Iterator<AtlasIndexQuery.Result> indexResultsIterator;
        private Iterator<AtlasVertex>            gremlinResultIterator;

        private Map<String, AtlasIndexQuery> cachedIndexQueries       = new HashMap<>();
        private Map<String, AtlasGraphQuery> cachedGraphQueries       = new HashMap<>();
        private Map<String, AtlasGraphTraversal> cachedGraphTraversal = new HashMap<>();

        public PipelineContext(SearchParameters searchParameters, AtlasEntityType entityType, AtlasClassificationType classificationType, Set<String> indexedKeys) {
            this.searchParameters   = searchParameters;
            this.entityType         = entityType;
            this.classificationType = classificationType;
            this.indexedKeys        = indexedKeys;

            currentOffset = searchParameters.getOffset();
            maxLimit      = searchParameters.getLimit();
        }

        public SearchParameters getSearchParameters() {
            return searchParameters;
        }

        public AtlasEntityType getEntityType() {
            return entityType;
        }

        public AtlasClassificationType getClassificationType() {
            return classificationType;
        }

        public Set<String> getIndexedKeys() { return indexedKeys; }

        public int getIterationCount() {
            return iterationCount;
        }

        public boolean getForceTerminate() {
            return forceTerminate;
        }

        public void setForceTerminate(boolean forceTerminate) {
            this.forceTerminate = forceTerminate;
        }

        public boolean hasProcessedTagAttribute(String attributeName) {
            return tagAttrProcessedBySolr.contains(attributeName);
        }

        public boolean hasProcessedEntityAttribute(String attributeName) {
            return entityAttrProcessedBySolr.contains(attributeName);
        }

        public Iterator<AtlasIndexQuery.Result> getIndexResultsIterator() {
            return indexResultsIterator;
        }

        public void setIndexResultsIterator(Iterator<AtlasIndexQuery.Result> indexResultsIterator) {
            this.indexResultsIterator = indexResultsIterator;
        }

        public Iterator<AtlasVertex> getGremlinResultIterator() {
            return gremlinResultIterator;
        }

        public void setGremlinResultIterator(Iterator<AtlasVertex> gremlinResultIterator) {
            this.gremlinResultIterator = gremlinResultIterator;
        }

        public boolean hasIndexResults() {
            return null != indexResultsIterator && indexResultsIterator.hasNext();
        }

        public boolean hasGremlinResults() {
            return null != gremlinResultIterator && gremlinResultIterator.hasNext();
        }


        public boolean isTagProcessingComplete() {
            return CollectionUtils.isEmpty(tagSearchAttributes) ||
                    CollectionUtils.isEqualCollection(tagSearchAttributes, tagAttrProcessedBySolr);
        }

        public boolean isEntityProcessingComplete() {
            return CollectionUtils.isEmpty(entitySearchAttributes) ||
                    CollectionUtils.isEqualCollection(entitySearchAttributes, entityAttrProcessedBySolr);
        }

        public boolean isProcessingComplete() {
            return isTagProcessingComplete() && isEntityProcessingComplete();
        }

        public void incrementOffset(int increment) {
            currentOffset += increment;
        }

        public void incrementSearchRound() {
            iterationCount ++;
            incrementOffset(searchParameters.getLimit());
        }

        public int getCurrentOffset() {
            return currentOffset;
        }

        public boolean addTagSearchAttribute(String attribute) {
            return tagSearchAttributes.add(attribute);
        }

        public boolean addProcessedTagAttribute(String attribute) {
            return tagAttrProcessedBySolr.add(attribute);
        }

        public boolean addEntitySearchAttribute(String attribute) {
            return entitySearchAttributes.add(attribute);
        }

        public boolean addProcessedEntityAttribute(String attribute) {
            return entityAttrProcessedBySolr.add(attribute);
        }

        public Set<String> getEntitySearchAttribute() {
            return entitySearchAttributes;
        }

        public void cacheGraphQuery(String name, AtlasGraphQuery graphQuery) {
            cachedGraphQueries.put(name, graphQuery);
        }

        public void cacheIndexQuery(String name, AtlasIndexQuery indexQuery) {
            cachedIndexQueries.put(name, indexQuery);
        }

        public void cacheGraphTraversal(String name, AtlasGraphTraversal graphTraversal) {
            cachedGraphTraversal.put(name, graphTraversal);
        }

        public AtlasIndexQuery getIndexQuery(String name){
            return cachedIndexQueries.get(name);
        }

        public AtlasGraphQuery getGraphQuery(String name) {
            return cachedGraphQueries.get(name);
        }

        public AtlasGraphTraversal getGraphTraversal(String name) {
            return cachedGraphTraversal.get(name);
        }

        public IndexResultType getIndexResultType() {
            return indexResultType;
        }

        public void setIndexResultType(IndexResultType indexResultType) {
            this.indexResultType = indexResultType;
        }

        public int getMaxLimit() {
            return maxLimit;
        }

        public ReferredEntityContext getReferredEntityContext() {
            return referredEntityContext;
        }

        public void addReferredEntityAttr(String entity, String attr, String qualifiedAttr) {
            referredEntityContext.addReferredAttr(entity, attr, qualifiedAttr);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("iterationCount", iterationCount)
                    .append("forceTerminate", forceTerminate)
                    .append("currentOffset", currentOffset)
                    .append("maxLimit", maxLimit)
                    .append("searchParameters", searchParameters)
                    .append("tagSearchAttributes", tagSearchAttributes)
                    .append("entitySearchAttributes", entitySearchAttributes)
                    .append("tagAttrProcessedBySolr", tagAttrProcessedBySolr)
                    .append("entityAttrProcessedBySolr", entityAttrProcessedBySolr)
                    .append("indexResultType", indexResultType)
                    .append("cachedIndexQueries", cachedIndexQueries)
                    .append("cachedGraphQueries", cachedGraphQueries)
                    .toString();
        }

        class ReferredEntityContext {
            private Set<String> referredEntitySearchAttributes                  = new HashSet<>();
            private Map<String, List<String>> referredEntityAttributes          = new HashMap<>();
            private Map<String, String> qualifiedAttrToEntityMap                = new HashMap<>();

            public void addReferredAttr(String entity, String attr, String qualifiedName) {
                List<String> entityAttributes = referredEntityAttributes.get(entity);
                if (entityAttributes == null) {
                    entityAttributes = new ArrayList<>();
                }
                entityAttributes.add(attr);
                referredEntityAttributes.put(entity, entityAttributes);
                referredEntitySearchAttributes.add(qualifiedName);
                qualifiedAttrToEntityMap.put(qualifiedName, entity);
            }

            public boolean isReferredEntityAttribute(String qualifiedAttrName) {
                return CollectionUtils.isNotEmpty(referredEntitySearchAttributes)
                        && referredEntitySearchAttributes.contains(qualifiedAttrName);
            }

            public Set<String> getReferredAttributes() {
                return referredEntitySearchAttributes;
            }

            public List<String> getReferredAttributes(String entity) {
                return referredEntityAttributes.get(entity);
            }

            public Set<String> getReferredEntities() {
                return referredEntityAttributes.keySet();
            }

            public String getEntityForAttr(String qualifiedAttr) {
                return qualifiedAttrToEntityMap.get(qualifiedAttr);
            }
        }
    }

    // Basic assumption is that there will only be one level of reference, one vertex hop
    public static class Util {
        public static boolean isReferredEntityAttr(AtlasStructType structType, String attributeName) {
            boolean ret = false;
            if (attributeName.contains(".")) {
                String[] attrParts = attributeName.split("\\.");

                String referredEntity = attrParts[0];
                String referredAttribute = attrParts[1];

                AtlasType attributeType = structType.getAttributeType(referredEntity.toLowerCase());
                if (attributeType != null) {
                    attributeType = structType.getAttributeType(referredEntity);
                }
                ret = isReferredAttribute(referredAttribute, attributeType);
            }

            return ret;
        }

        public static String toNonQualifiedName(String attrName) {
            String ret;
            if (attrName.contains(".")) {
                String[] attributeParts = attrName.split("\\.");
                ret = attributeParts[attributeParts.length - 1];
            } else {
                ret = attrName;
            }
            return ret;
        }

        private static boolean isReferredAttribute(String attrName, AtlasType attributeType) {
            boolean ret = false;
            if (attributeType != null && attributeType instanceof AtlasStructType) {
                AtlasStructType atlasStructType = (AtlasStructType) attributeType;
                AtlasStructType.AtlasAttribute referredAttribute = atlasStructType.getAttribute(attrName);
                ret = referredAttribute != null;
            }
            return ret;
        }
    }
}
