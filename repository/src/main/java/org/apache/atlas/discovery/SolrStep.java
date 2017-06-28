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

import org.apache.atlas.discovery.SearchPipeline.IndexResultType;
import org.apache.atlas.discovery.SearchPipeline.PipelineContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;

@Component
public class SolrStep implements SearchPipeline.PipelineStep {
    private static final String AND_STR = " AND ";
    private static final Logger LOG = LoggerFactory.getLogger(SolrStep.class);
    private static final Pattern STRAY_AND_PATTERN = Pattern.compile("(AND\\s+)+\\)");
    private static final Pattern STRAY_OR_PATTERN = Pattern.compile("(OR\\s+)+\\)");
    private static final Pattern STRAY_ELIPSIS_PATTERN = Pattern.compile("(\\(\\s*)\\)");
    private static final String EMPTY_STRING = "";
    private static final String SPACE_STRING = " ";
    private static final String BRACE_OPEN_STR = "( ";
    private static final String BRACE_CLOSE_STR = " )";
    public static final String STATUS_ACTIVE = "ACTIVE";
    public static final String STATUS_DELETED = "DELETED";
    public static final String VERTEX_INDEX_CACHE_NAME = "VERTEX_INDEX";
    public static final String FULLTEXT_CACHE_NAME = "FULLTEXT";

    private Map<Operator, String> operatorMap = new HashMap<>();

    {
        operatorMap.put(Operator.LT,"v.\"%s\": [* TO %s}");
        operatorMap.put(Operator.GT,"v.\"%s\": {%s TO *]");
        operatorMap.put(Operator.LTE,"v.\"%s\": [* TO %s]");
        operatorMap.put(Operator.GTE,"v.\"%s\": [%s TO *]");
        operatorMap.put(Operator.EQ,"v.\"%s\": %s");
        operatorMap.put(Operator.NEQ,"v.\"%s\": (NOT %s)");
        operatorMap.put(Operator.IN, "v.\"%s\": (%s)");
        operatorMap.put(Operator.LIKE, "v.\"%s\": (%s)");
        operatorMap.put(Operator.STARTS_WITH, "v.\"%s\": (%s*)");
        operatorMap.put(Operator.ENDS_WITH, "v.\"%s\": (*%s)");
        operatorMap.put(Operator.CONTAINS, "v.\"%s\": (*%s*)");
    }

    private final AtlasGraph graph;
    private final AtlasTypeRegistry atlasTypeRegistry;
    private final GraphBackedSearchIndexer indexer;

    @Inject
    public SolrStep(AtlasGraph graph, AtlasTypeRegistry atlasTypeRegistry, GraphBackedSearchIndexer indexer) {
        this.graph = graph;
        this.atlasTypeRegistry = atlasTypeRegistry;
        this.indexer = indexer;
    }

    @Override
    public void execute(PipelineContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()){
            LOG.debug("==> execute({})", context);
        }

        if (context != null) {
            LOG.debug("Can't process any prior results for this search step");
        } else {
            throw new AtlasBaseException("Can't start search without any context");
        }

        // TODO: Index search for tag attributes (query and entity filter are missing)
        SearchParameters searchParameters = context.getSearchParameters();
        if (StringUtils.isNotEmpty(searchParameters.getQuery())) {
            LOG.warn("Only processing Fulltext query");
            executeAgainstFulltextIndex(context);
        } else {
            executeAgainstVertexIndex(context);
        }

        if (LOG.isDebugEnabled()){
            LOG.debug("<== {} execute(..)", context);
        }
    }

    private void executeAgainstFulltextIndex(PipelineContext context) {
        AtlasIndexQuery query = context.getIndexQuery(FULLTEXT_CACHE_NAME);
        if (query == null) {
            // Compute only once
            SearchParameters searchParameters = context.getSearchParameters();
            String indexQuery = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, searchParameters.getQuery());
            if (searchParameters.getExcludeDeletedEntities()) {
                indexQuery += " AND __state:(ACTIVE)";
            }
            query = graph.indexQuery(Constants.FULLTEXT_INDEX, indexQuery);
            context.cacheIndexQuery(FULLTEXT_CACHE_NAME, query);
        }

        Iterator vertices = query.vertices(context.getCurrentOffset(), context.getMaxLimit());
        context.setIndexResultType(IndexResultType.ENTITY);
        context.setIndexResultsIterator(vertices);
    }

    private void executeAgainstVertexIndex(PipelineContext context) {
        Iterator<AtlasIndexQuery.Result> ret;
        SearchParameters searchParameters = context.getSearchParameters();

        AtlasIndexQuery vertexIndexQuery = context.getIndexQuery(VERTEX_INDEX_CACHE_NAME);
        StringBuilder solrQuery = new StringBuilder();

        if (vertexIndexQuery == null) {
            if (StringUtils.isNotEmpty(searchParameters.getClassification())) {
                // If tag is specified then let's start processing using tag and it's attributes, entity filters will
                // be pushed to Gremlin
                LOG.debug("Solr processing Tag and it's attributes");
                constructTypeTestQuery(solrQuery, searchParameters.getClassification(), IndexResultType.TAG, context);
                constructFilterQuery(solrQuery, searchParameters.getClassification(), searchParameters.getTagFilters(), context);
            } else {
                // No tag relate stuff to be processed, let's try entity processing
                LOG.debug("Solr processing Entity and it's attributes");
                if (StringUtils.isNotEmpty(searchParameters.getTypeName())) {
                    constructTypeTestQuery(solrQuery, searchParameters.getTypeName(), IndexResultType.ENTITY, context);
                    constructFilterQuery(solrQuery, searchParameters.getTypeName(), searchParameters.getEntityFilters(), context);
                }
            }
            // No query was formed, doesn't make sense to do anything beyond this point
            if (solrQuery.length() == 0) return;

            // Execute solr query and return the index results in the context
            vertexIndexQuery = pruneSolrQuery(context, vertexIndexQuery, solrQuery);
            // Set the status flag
            if (searchParameters.getExcludeDeletedEntities()) {
                solrQuery.append(" AND __state: (ACTIVE)");
            }
        }

        ret = vertexIndexQuery.vertices(context.getCurrentOffset(), context.getMaxLimit());
        context.setIndexResultsIterator(ret);
    }

    private void constructTypeTestQuery(StringBuilder solrQuery, String typeName, IndexResultType type, PipelineContext context) {
        Set<String> typeAndAllSubTypes;
        String typeAndSubtypesString;

        if (IndexResultType.TAG == type) {
            AtlasClassificationType tagType = atlasTypeRegistry.getClassificationTypeByName(typeName);
            if (tagType == null) {
                return;
            }
            typeAndAllSubTypes = tagType.getTypeAndAllSubTypes();
        } else {
            AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName(typeName);
            if (entityType == null) {
                return;
            }
            typeAndAllSubTypes = entityType.getTypeAndAllSubTypes();
        }

        typeAndSubtypesString = StringUtils.join(typeAndAllSubTypes, SPACE_STRING);
        solrQuery.append("v.\"__typeName\": (")
                .append(typeAndSubtypesString)
                .append(")");
        context.setIndexResultType(type);
    }

    private void constructFilterQuery(StringBuilder solrQuery, String typeName, FilterCriteria filterCriteria, PipelineContext context) {
        if (null != filterCriteria) {
            LOG.debug("Processing Filters");
            String filterQuery = toSolrQuery(typeName, filterCriteria, context);
            if (StringUtils.isNotEmpty(filterQuery)) {
                solrQuery.append(AND_STR).append(filterQuery);
            }
        }
    }

    private AtlasIndexQuery pruneSolrQuery(PipelineContext context, AtlasIndexQuery vertexIndexQuery, StringBuilder solrQuery) {
        String validSolrQuery = STRAY_AND_PATTERN.matcher(solrQuery).replaceAll(")");
        validSolrQuery = STRAY_OR_PATTERN.matcher(validSolrQuery).replaceAll(")");
        validSolrQuery = STRAY_ELIPSIS_PATTERN.matcher(validSolrQuery).replaceAll(EMPTY_STRING);

        if (solrQuery.length() > 0) {
            vertexIndexQuery = graph.indexQuery(Constants.VERTEX_INDEX, validSolrQuery);
            context.cacheIndexQuery(VERTEX_INDEX_CACHE_NAME, vertexIndexQuery);
        }
        return vertexIndexQuery;
    }

    private String toSolrQuery(String typeName, FilterCriteria criteria, PipelineContext context) {
        return toSolrQuery(typeName, criteria, context, new StringBuilder());
    }

    private String toSolrQuery(String typeName, FilterCriteria criteria, PipelineContext context, StringBuilder sb) {
        if (null != criteria.getCondition()) {
            StringBuilder nestedExpression = sb.append(BRACE_OPEN_STR);
            List<SearchParameters.FilterCriteria> criterion = criteria.getCriterion();
            boolean hasNestedQuery = false;
            for (int i = 0; i < criterion.size(); i++) {
                FilterCriteria filterCriteria = criterion.get(i);
                String nestedQuery = toSolrQuery(typeName, filterCriteria, context);
                if (!nestedQuery.isEmpty()) {
                    hasNestedQuery = true;
                    nestedExpression.append(nestedQuery);
                    if (i < criterion.size() - 1) {
                        nestedExpression.append(SPACE_STRING).append(criteria.getCondition()).append(SPACE_STRING);
                    }
                }
            }
            return hasNestedQuery ? nestedExpression.append(BRACE_CLOSE_STR).toString() : EMPTY_STRING;
        } else {
            return toSolrExpression(typeName, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue(), context);
        }
    }

    private String toSolrExpression(String typeName, String attrName, Operator op, String attrVal, PipelineContext context) {
        String indexKey = null;
        AtlasType attributeType = null;

        switch (context.getIndexResultType()) {
            case TAG:
                AtlasClassificationType tagType = atlasTypeRegistry.getClassificationTypeByName(typeName);
                if (tagType == null) {
                    return EMPTY_STRING;
                }

                try {
                    indexKey = tagType.getQualifiedAttributeName(attrName);
                    // Keep a track of all attributes that need to be processed processing
                    context.addTagSearchAttribute(indexKey);
                } catch (AtlasBaseException ex) {
                    LOG.warn(ex.getMessage());
                }
                attributeType = tagType.getAttributeType(attrName);
                break;
            case ENTITY:
                AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName(typeName);
                if (entityType == null) {
                    return EMPTY_STRING;
                }

                try {
                    indexKey = entityType.getQualifiedAttributeName(attrName);
                    // Keep a track of all attributes that need to be processed processing
                    context.addEntitySearchAttribute(indexKey);
                } catch (AtlasBaseException ex) {
                    LOG.warn(ex.getMessage());
                }
                attributeType = entityType.getAttributeType(attrName);
                break;
            default:
                // Do nothing
        }

        if (null == attributeType) return EMPTY_STRING;

        if (AtlasTypeUtil.isBuiltInType(attributeType.getTypeName()) && indexer.getVertexIndexKeys().contains(indexKey)) {
            if (null != operatorMap.get(op)) {
                // If there's a chance of multi-value then we need some additional processing here
                switch (context.getIndexResultType()) {
                    case TAG:
                        context.addProcessedTagAttribute(indexKey);
                        break;
                    case ENTITY:
                        context.addProcessedEntityAttribute(indexKey);
                        break;
                }
                return String.format(operatorMap.get(op), indexKey, attrVal);
            }
        }
        return EMPTY_STRING;
    }
}
