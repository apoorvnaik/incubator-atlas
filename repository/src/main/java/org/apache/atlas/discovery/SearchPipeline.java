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
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.SearchTracker;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
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

@Component
public class SearchPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(SearchPipeline.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("SearchPipeline");

    private final PipelineStep gremlinStep;
    private final PipelineStep solrStep;
    private final SearchTracker searchTracker;
    private final AtlasTypeRegistry typeRegistry;
    private final Configuration atlasConfiguration;
    private final GraphBackedSearchIndexer indexer;

    @Inject
    public SearchPipeline(SolrStep solrStep, GremlinStep gremlinStep, SearchTracker searchTracker,
                          AtlasTypeRegistry typeRegistry, Configuration atlasConfiguration, GraphBackedSearchIndexer indexer) {
        this.solrStep = solrStep;
        this.gremlinStep = gremlinStep;
        this.searchTracker = searchTracker;
        this.typeRegistry = typeRegistry;
        this.atlasConfiguration = atlasConfiguration;
        this.indexer = indexer;
    }

    // TODO: Maybe expose another method which describes the execution plan with some additional metadata

    public List<AtlasVertex> run(SearchParameters searchParameters) throws AtlasBaseException {
        List<AtlasVertex> ret;

        PipelineContext context = new PipelineContext(searchParameters);
        // For future cancellation
        String searchId = searchTracker.add(context);

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
        }

        return ret;
    }

    private List<AtlasVertex> runOnlySolr(PipelineContext context) throws AtlasBaseException {
        // Only when there's no tag and query
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "SearchPipeline.runOnlySolr("+ context +")");
        }

        List<AtlasVertex> results = new ArrayList<>();

        while (results.size() < context.getSearchParameters().getLimit()) {
            if (LOG.isDebugEnabled()){
                LOG.debug("Pipeline iteration {} ", context.getIterationCount());
            }

            if (context.getForceTerminate()) {
                LOG.warn("Pipeline ordered to terminate");
                break;
            }

            // Execute solr search only
            solrStep.execute(context);
            if (!context.hasIndexResults()) {
                if (LOG.isDebugEnabled()){
                    LOG.debug("No index results in iteration {}", context.getIterationCount());
                }
                // If no result is found any subsequent iteration then just stop querying the index
                break;
            }

            results.addAll(getIndexResults(context.getSearchParameters(), context));
            context.incrementSearchRound();
        }

        AtlasPerfTracer.log(perf);
        return results;
    }

    private List<AtlasVertex> runOnlyGremlin(PipelineContext context) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "SearchPipeline.runOnlyGremlin("+ context +")");
        }

        List<AtlasVertex> resultSet = new ArrayList<>();

        while (resultSet.size() < context.getSearchParameters().getLimit()) {
            if (LOG.isDebugEnabled()){
                LOG.debug("Pipeline iteration {} ", context.getIterationCount());
            }

            if (context.getForceTerminate()) {
                LOG.warn("Pipeline ordered to force terminate");
                break;
            }

            gremlinStep.execute(context);
            resultSet = captureGremlinResults(context);
            context.incrementSearchRound();
        }

        AtlasPerfTracer.log(perf);
        return resultSet;
    }

    private List<AtlasVertex> runMixed(PipelineContext context) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "SearchPipeline.runMixed("+ context +")");
        }
        /*
            1. Index processes few attributes and then gremlin processes rest
                1.1 Iterate for gremlin till the index results are non null
            2. Index processes all attributes, gremlin has nothing to do

            Sometimes the result set might be less than the max limit and we need to iterate until the result set is full
            or the iteration doesn't return any results

         */
        List<AtlasVertex> resultSet = new ArrayList<>();

        while (resultSet.size() < context.getSearchParameters().getLimit()) {
            if (LOG.isDebugEnabled()){
                LOG.debug("Pipeline iteration {} ", context.getIterationCount());
            }

            if (context.getForceTerminate()) {
                LOG.warn("Pipeline ordered to terminate");
                break;
            }
            // Execute Solr search and then pass it to the Gremlin step (if needed)
            solrStep.execute(context);

            if (!context.hasIndexResults()) {
                if (LOG.isDebugEnabled()){
                    LOG.debug("No index results in iteration {}", context.getIterationCount());
                }
                // If no result is found any subsequent iteration then just stop querying the index
                break;
            }

            // Attributes partially processed by Solr, use gremlin to process remaining attribute(s)
            gremlinStep.execute(context);
            resultSet = captureGremlinResults(context);
            context.incrementSearchRound();
        }

        AtlasPerfTracer.log(perf);
        return resultSet;
    }

    private List<AtlasVertex> getIndexResults(SearchParameters searchParameters, PipelineContext pipelineContext) {
        List<AtlasVertex> resultSet = new ArrayList<>();

        if (pipelineContext.hasIndexResults()) {
            Iterator<AtlasIndexQuery.Result> indexResultsIterator = pipelineContext.getIndexResultsIterator();
            while(indexResultsIterator.hasNext() && resultSet.size() < searchParameters.getLimit()) {
                resultSet.add(indexResultsIterator.next().getVertex());
            }
        }

        return resultSet;
    }

    private List<AtlasVertex> captureGremlinResults(PipelineContext context) {
        List<AtlasVertex> ret = Collections.emptyList();

        if (!context.hasGremlinResults()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Gremlin search has no results. Further iterations cancelled");
            }
        } else {
            Iterator<AtlasVertex> vertexResults = context.getGremlinResultIterator();
            // Add all results from the gremlin step
            while (vertexResults.hasNext() && ret.size() < context.getSearchParameters().getLimit()) {
                AtlasVertex vertex = vertexResults.next();
                // Only add the Vertex if it has a GUID
                if (StringUtils.isNotEmpty(AtlasGraphUtilsV1.getIdFromVertex(vertex))) {
                    ret.add(vertex);
                }
            }
        }
        return ret;
    }

    private ExecutionMode determineExecutionMode(PipelineContext context) {
        /*
            1. Max tags/types limit exceeded - Gremlin
            2. Non-indexed Nested OR within AND - Gremlin
         */
        AtlasClassificationType tagType = null;
        AtlasEntityType entityType = null;
        Set<String> indexKeys = indexer.getVertexIndexKeys();
        ExecutionMode mode = ExecutionMode.MIXED; // default we'll use index + gremlin

        SearchParameters searchParameters = context.getSearchParameters();
        if (StringUtils.isNotEmpty(searchParameters.getClassification())) {
            tagType = typeRegistry.getClassificationTypeByName(searchParameters.getClassification());
            if (null != tagType)  {
                Set<String> typeAndAllSubTypes = tagType.getTypeAndAllSubTypes();
                if (typeAndAllSubTypes.size() > atlasConfiguration.getInt(Constants.INDEX_SEARCH_MAX_TAGS_COUNT, 10)) {
                    mode = ExecutionMode.GREMLIN;
                    LOG.warn("Tag and subtypes exceed max limit");
                    return mode;
                }
            }
        }

        if (StringUtils.isNotEmpty(searchParameters.getTypeName())) {
            entityType = typeRegistry.getEntityTypeByName(searchParameters.getTypeName());
            if (null != entityType) {
                Set<String> typeAndAllSubTypes = entityType.getTypeAndAllSubTypes();
                if (typeAndAllSubTypes.size() > atlasConfiguration.getInt(Constants.INDEX_SEARCH_MAX_TYPES_COUNT, 10)) {
                    mode = ExecutionMode.GREMLIN;
                    LOG.warn("Type and subtypes exceed max limit");
                    return mode;
                }
            }
        }

        // If Index can't process all attributes and any of the non-indexed attribute is present in OR nested within AND
        // then the only way is Gremlin
        // A violation (here) is defined as presence of non-indexed attribute within any OR clause nested under an AND clause
        // the reason being that the index would not be able to process the nested OR attribute which might result in
        // exclusion of valid result (vertex)
        boolean tagAttrViolation = hasNonIndexedAttrViolation(tagType, indexKeys, searchParameters.getTagFilters(), null);
        boolean entityAttrViolation = hasNonIndexedAttrViolation(entityType, indexKeys, searchParameters.getEntityFilters(), null);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Tag Violation: {}", tagAttrViolation);
            LOG.debug("Entity Violation: {}", entityAttrViolation);
        }
        if (tagAttrViolation || entityAttrViolation) {
            mode = ExecutionMode.GREMLIN;
        }

        return mode;
    }

    private boolean hasNonIndexedAttrViolation(AtlasStructType structType, Set<String> indexKeys, FilterCriteria filterCriteria, Condition parentCondition) {
        if (null == structType || CollectionUtils.isEmpty(indexKeys) || null == filterCriteria)
            return false;

        boolean ret = false;
        Condition filterCondition = filterCriteria.getCondition();

        if (null != filterCondition) {
            // If we have nested criterion let's find any nested ORs with non-indexed attr
            List<FilterCriteria> criterion = filterCriteria.getCriterion();
            for (FilterCriteria criteria : criterion) {
                ret |= hasNonIndexedAttrViolation(structType, indexKeys, criteria, filterCondition);
            }
        } else {
            // If attribute qualified name doesn't exist in the vertex index we potentially might have a problem
            try {
                String qualifiedAttributeName = structType.getQualifiedAttributeName(filterCriteria.getAttributeName());
                return !indexKeys.contains(qualifiedAttributeName);
            } catch (AtlasBaseException e) {
                LOG.warn(e.getMessage());
                ret = true;
            }
        }

        if (parentCondition == null) {
            return ret;
        } else {
            return ret && Condition.OR == filterCondition && Condition.AND == parentCondition;
        }
    }

    public interface PipelineStep {
        void execute(PipelineContext context) throws AtlasBaseException;
    }

    enum ExecutionMode { SOLR, MIXED, GREMLIN }

    enum IndexResultType { TAG, ENTITY }

    public static class PipelineContext {
        // TODO: See if anything can be cached in the context

        private int iterationCount;
        private boolean forceTerminate;
        private int currentOffset;
        private int maxLimit;

        private SearchParameters searchParameters;

        // Continuous processing stuff
        private Set<String> tagSearchAttributes = new HashSet<>();
        private Set<String> entitySearchAttributes = new HashSet<>();
        private Set<String> tagAttrProcessedBySolr = new HashSet<>();
        private Set<String> entityAttrProcessedBySolr = new HashSet<>();

        // Results related stuff
        private IndexResultType indexResultType;
        private Iterator<AtlasIndexQuery.Result> indexResultsIterator;
        private Iterator<AtlasVertex> gremlinResultIterator;

        private Map<String, AtlasIndexQuery> cachedIndexQueries = new HashMap<>();
        private Map<String, AtlasGraphQuery> cachedGraphQueries = new HashMap<>();

        public PipelineContext(SearchParameters searchParameters) {
            this.searchParameters = searchParameters;
            currentOffset = searchParameters.getOffset();
            maxLimit = searchParameters.getLimit();
        }

        public int getIterationCount() {
            return iterationCount;
        }

        public boolean getForceTerminate() {
            return forceTerminate;
        }

        public void setForceTerminate(boolean forceTerminate) {
            this.forceTerminate= forceTerminate;
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
            return tagSearchAttributes.add(attribute);
        }

        public boolean addProcessedEntityAttribute(String attribute) {
            return entityAttrProcessedBySolr.add(attribute);
        }

        public SearchParameters getSearchParameters() {
            return searchParameters;
        }

        public void cacheGraphQuery(String name, AtlasGraphQuery graphQuery) {
            cachedGraphQueries.put(name, graphQuery);
        }

        public void cacheIndexQuery(String name, AtlasIndexQuery indexQuery) {
            cachedIndexQueries.put(name, indexQuery);
        }

        public AtlasIndexQuery getIndexQuery(String name){
            return cachedIndexQueries.get(name);
        }

        public AtlasGraphQuery getGraphQuery(String name) {
            return cachedGraphQueries.get(name);
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
    }
}
