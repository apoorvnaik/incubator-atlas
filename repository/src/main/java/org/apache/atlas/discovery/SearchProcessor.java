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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public abstract class SearchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SearchProcessor.class);

    public static final Pattern STRAY_AND_PATTERN                       = Pattern.compile("(AND\\s+)+\\)");
    public static final Pattern STRAY_OR_PATTERN                        = Pattern.compile("(OR\\s+)+\\)");
    public static final Pattern STRAY_ELIPSIS_PATTERN                   = Pattern.compile("(\\(\\s*)\\)");
    public static final int     MAX_RESULT_SIZE                         = getApplicationProperty(Constants.INDEX_SEARCH_MAX_RESULT_SET_SIZE, 150);
    public static final int     MAX_ENTITY_TYPES_IN_INDEX_QUERY         = getApplicationProperty(Constants.INDEX_SEARCH_MAX_TYPES_COUNT, 10);
    public static final int     MAX_CLASSIFICATION_TYPES_IN_INDEX_QUERY = getApplicationProperty(Constants.INDEX_SEARCH_MAX_TAGS_COUNT, 10);
    public static final String  AND_STR         = " AND ";
    public static final String  EMPTY_STRING    = "";
    public static final String  SPACE_STRING    = " ";
    public static final String  BRACE_OPEN_STR  = "( ";
    public static final String  BRACE_CLOSE_STR = " )";

    private static final Map<SearchParameters.Operator, String> OPERATOR_MAP = new HashMap<>();
    private static final char[] OFFENDING_CHARS = {'@', '/', ' '}; // This can grow as we discover corner cases

    static
    {
        OPERATOR_MAP.put(SearchParameters.Operator.LT,"v.\"%s\": [* TO %s}");
        OPERATOR_MAP.put(SearchParameters.Operator.GT,"v.\"%s\": {%s TO *]");
        OPERATOR_MAP.put(SearchParameters.Operator.LTE,"v.\"%s\": [* TO %s]");
        OPERATOR_MAP.put(SearchParameters.Operator.GTE,"v.\"%s\": [%s TO *]");
        OPERATOR_MAP.put(SearchParameters.Operator.EQ,"v.\"%s\": %s");
        OPERATOR_MAP.put(SearchParameters.Operator.NEQ,"-" + "v.\"%s\": %s");
        OPERATOR_MAP.put(SearchParameters.Operator.IN, "v.\"%s\": (%s)"); // this should be a list of quoted strings
        OPERATOR_MAP.put(SearchParameters.Operator.LIKE, "v.\"%s\": (%s)"); // this should be regex pattern
        OPERATOR_MAP.put(SearchParameters.Operator.STARTS_WITH, "v.\"%s\": (%s*)");
        OPERATOR_MAP.put(SearchParameters.Operator.ENDS_WITH, "v.\"%s\": (*%s)");
        OPERATOR_MAP.put(SearchParameters.Operator.CONTAINS, "v.\"%s\": (*%s*)");
    }

    protected final SearchContext   context;
    protected       SearchProcessor nextProcessor;


    protected SearchProcessor(SearchContext context) {
        this.context = context;
    }

    public void addProcessor(SearchProcessor processor) {
        if (nextProcessor == null) {
            nextProcessor = processor;
        } else {
            nextProcessor.addProcessor(processor);
        }
    }

    public abstract List<AtlasVertex> execute();

    public List<AtlasVertex> filter(List<AtlasVertex> entityVertices) {
        return nextProcessor == null || CollectionUtils.isEmpty(entityVertices) ? entityVertices : nextProcessor.filter(entityVertices);
    }


    protected void processSearchAttributes(AtlasStructType structType, FilterCriteria filterCriteria, Set<String> solrFiltered, Set<String> gremlinFiltered, Set<String> allAttributes) {
        if (structType == null || filterCriteria == null) {
            return;
        }

        Condition            filterCondition = filterCriteria.getCondition();
        List<FilterCriteria> criterion       = filterCriteria.getCriterion();

        if (filterCondition != null && CollectionUtils.isNotEmpty(criterion)) {
            for (SearchParameters.FilterCriteria criteria : criterion) {
                processSearchAttributes(structType, criteria, solrFiltered, gremlinFiltered, allAttributes);
            }
        } else if (StringUtils.isNotEmpty(filterCriteria.getAttributeName())) {
            try {
                String      attributeName = filterCriteria.getAttributeName();
                String      qualifiedName = structType.getQualifiedAttributeName(attributeName);
                Set<String> indexedKeys   = context.getIndexedKeys();

                if (indexedKeys != null && indexedKeys.contains(qualifiedName)) {
                    solrFiltered.add(attributeName);
                } else {
                    LOG.warn("search includes non-indexed attribute '{}'; might cause poor performance", qualifiedName);

                    gremlinFiltered.add(attributeName);
                }

                if (structType instanceof AtlasEntityType) {
                    // Capture the entity attributes
                    context.getEntityAttributes().add(attributeName);
                }

                allAttributes.add(attributeName);
            } catch (AtlasBaseException e) {
                LOG.warn(e.getMessage());
            }
        }
    }

    //
    // If filterCriteria contains any non-indexed attribute inside OR condition:
    //    Solr+Grelin can't be used. Need to use only Gremlin filter for all attributes. Examples:
    //    (OR idx-att1=x non-idx-attr=z)
    //    (AND idx-att1=x (OR idx-attr2=y non-idx-attr=z))
    // Else
    //    Solr can be used for indexed-attribute filtering and Gremlin for non-indexed attributes. Examples:
    //      (AND idx-att1=x idx-attr2=y non-idx-attr=z)
    //      (AND (OR idx-att1=x idx-attr1=y) non-idx-attr=z)
    //      (AND (OR idx-att1=x idx-attr1=y) non-idx-attr=z (AND idx-attr2=xyz idx-attr2=abc))
    //
    protected boolean canApplySolrFilter(AtlasStructType structType, FilterCriteria filterCriteria, boolean insideOrCondition) {
        if (filterCriteria == null) {
            return true;
        }

        boolean              ret             = true;
        Condition            filterCondition = filterCriteria.getCondition();
        List<FilterCriteria> criterion       = filterCriteria.getCriterion();
        Set<String>          indexedKeys     = context.getIndexedKeys();


        if (filterCondition != null && CollectionUtils.isNotEmpty(criterion)) {
            insideOrCondition = insideOrCondition || filterCondition == Condition.OR;

            // If we have nested criterion let's find any nested ORs with non-indexed attr
            for (FilterCriteria criteria : criterion) {
                ret = canApplySolrFilter(structType, criteria, insideOrCondition);

                if (!ret) {
                    break;
                }
            }
        } else if (StringUtils.isNotEmpty(filterCriteria.getAttributeName())) {
            try {
                String qualifiedName = structType.getQualifiedAttributeName(filterCriteria.getAttributeName());

                if (insideOrCondition && (indexedKeys == null || !indexedKeys.contains(qualifiedName))) {
                    ret = false;
                }
            } catch (AtlasBaseException e) {
                LOG.warn(e.getMessage());
            }
        }

        return ret;
    }

    protected void constructTypeTestQuery(StringBuilder solrQuery, Set<String> typeAndAllSubTypes) {
        String typeAndSubtypesString = StringUtils.join(typeAndAllSubTypes, SPACE_STRING);

        solrQuery.append("v.\"__typeName\": (")
                .append(typeAndSubtypesString)
                .append(")");
    }

    protected void constructTypeTestQuery(BooleanQuery booleanClauses, Set<String> typeAndAllSubTypes) {
        String typeAndSubtypesString = StringUtils.join(typeAndAllSubTypes, SPACE_STRING);
        TermQuery termQuery = new TermQuery(new Term("v.\"__typeName\"", typeAndSubtypesString));
        booleanClauses.add(termQuery, BooleanClause.Occur.MUST);
    }

    protected void constructFilterQuery(StringBuilder solrQuery, AtlasStructType type, FilterCriteria filterCriteria, Set<String> solrAttributes) {
        if (filterCriteria != null) {
            LOG.debug("Processing Filters");

            String filterQuery = toSolrQuery(type, filterCriteria, solrAttributes, 0);

            if (StringUtils.isNotEmpty(filterQuery)) {
                if (solrQuery.length() > 0) {
                    solrQuery.append(AND_STR);
                }

                solrQuery.append(filterQuery);
            }
        }

        if (type instanceof AtlasEntityType && context.getSearchParameters().getExcludeDeletedEntities()) {
            if (solrQuery.length() > 0) {
                solrQuery.append(AND_STR);
            }

            solrQuery.append("v.\"__state\":").append("ACTIVE");
        }
    }

    protected void constructFilterQuery(BooleanQuery booleanClauses, AtlasStructType type, FilterCriteria filterCriteria, Set<String> solrAttributes) {
        if (filterCriteria != null) {
            LOG.debug("Processing Filters");

            constructBooleanClause(booleanClauses, type, filterCriteria, solrAttributes);
        }

        if (type instanceof AtlasEntityType && context.getSearchParameters().getExcludeDeletedEntities()) {
            TermQuery activeStateQuery = new TermQuery(new Term("v.\"__state\"", "ACTIVE"));
            booleanClauses.add(activeStateQuery, BooleanClause.Occur.MUST);
        }
    }

    private void constructBooleanClause(BooleanQuery booleanClauses, AtlasStructType type, FilterCriteria criteria, Set<String> solrAttributes) {
        if (criteria.getCondition() != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            BooleanClause.Occur occur = criteria.getCondition() == Condition.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD;

            for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                BooleanQuery nestedBooleanClause = new BooleanQuery();
                constructBooleanClause(nestedBooleanClause, type, filterCriteria, solrAttributes);

                booleanClauses.add(nestedBooleanClause, occur);
            }
        } else if (solrAttributes.contains(criteria.getAttributeName())){
            constructAttrQuery(booleanClauses, type, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue());
        }
    }

    private void constructAttrQuery(BooleanQuery booleanClauses, AtlasStructType type, String attrName, SearchParameters.Operator operator, String attrVal) {
        Query attrQuery;
        String qualifiedName;
        try {
            qualifiedName = type.getQualifiedAttributeName(attrName);
        } catch (AtlasBaseException e) {
            LOG.warn("Attr {} doesn't have qualifiedName", attrName);
            qualifiedName = attrName;
        }

        String field = "v.\"" + qualifiedName + "\"";
        switch (operator) {
            case LT:
                attrQuery = NumericRangeQuery.newLongRange(field, null, Long.valueOf(attrVal), true, false);
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case GT:
                attrQuery = NumericRangeQuery.newLongRange(field, Long.valueOf(attrVal), null, false, true);
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case LTE:
                attrQuery = NumericRangeQuery.newLongRange(field, null, Long.valueOf(attrVal), true, true);
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case GTE:
                attrQuery = NumericRangeQuery.newLongRange(field, Long.valueOf(attrVal), null, true, false);
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case EQ:
                if (hasOffendingChars(attrVal)) {
                    attrQuery = new TermQuery(new Term(field, "\"" + attrVal + "\""));
                } else {
                    attrQuery = new TermQuery(new Term(field, attrVal));
                }
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case NEQ:
                attrQuery = new TermQuery(new Term(field, attrVal));
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST_NOT);
                break;
            case IN:
                PhraseQuery phraseQuery = new PhraseQuery();
                phraseQuery.add(new Term(field, attrVal));
                attrQuery = phraseQuery;
                booleanClauses.add(attrQuery, BooleanClause.Occur.SHOULD);
                break;
            case LIKE:
                attrQuery = new RegexpQuery(new Term(field, attrVal));
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case STARTS_WITH:
                attrQuery = new PrefixQuery(new Term(field, attrVal));
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case ENDS_WITH:
                attrQuery = new WildcardQuery(new Term(field, "*" + attrVal));
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
            case CONTAINS:
                attrQuery = new TermQuery(new Term(field, "*" + attrVal + "*"));
                booleanClauses.add(attrQuery, BooleanClause.Occur.MUST);
                break;
        }
    }

    private String toSolrQuery(AtlasStructType type, FilterCriteria criteria, Set<String> solrAttributes, int level) {
        return toSolrQuery(type, criteria, solrAttributes, new StringBuilder(), level);
    }

    private String toSolrQuery(AtlasStructType type, FilterCriteria criteria, Set<String> solrAttributes, StringBuilder sb, int level) {
        if (criteria.getCondition() != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            StringBuilder nestedExpression = new StringBuilder();

            for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                String nestedQuery = toSolrQuery(type, filterCriteria, solrAttributes, level + 1);

                if (StringUtils.isNotEmpty(nestedQuery)) {
                    if (nestedExpression.length() > 0) {
                        nestedExpression.append(SPACE_STRING).append(criteria.getCondition()).append(SPACE_STRING);
                    }
                    // todo: when a neq operation is nested and occurs in the beginning of the query, solr has issues
                    nestedExpression.append(nestedQuery);
                }
            }

            if (level == 0) {
                return nestedExpression.length() > 0 ? sb.append(nestedExpression).toString() : EMPTY_STRING;
            } else {
                return nestedExpression.length() > 0 ? sb.append(BRACE_OPEN_STR).append(nestedExpression).append(BRACE_CLOSE_STR).toString() : EMPTY_STRING;
            }
        } else if (solrAttributes.contains(criteria.getAttributeName())){
            return toSolrExpression(type, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue());
        } else {
            return EMPTY_STRING;
        }
    }

    private String toSolrExpression(AtlasStructType type, String attrName, SearchParameters.Operator op, String attrVal) {
        String ret = EMPTY_STRING;

        try {
            String qualifiedName = type.getQualifiedAttributeName(attrName);

            if (OPERATOR_MAP.get(op) != null) {
                if (hasOffendingChars(attrVal)) {
                    // FIXME: if attrVal has offending chars & op is contains, endsWith, startsWith, solr doesn't like it and results are skewed
                    ret = String.format(OPERATOR_MAP.get(op), qualifiedName, "\"" + attrVal + "\"");
                } else {
                    ret = String.format(OPERATOR_MAP.get(op), qualifiedName, attrVal);
                }
            }
        } catch (AtlasBaseException ex) {
            LOG.warn(ex.getMessage());
        }

        return ret;
    }

    protected AtlasGraphQuery toGremlinFilterQuery(AtlasStructType type, FilterCriteria criteria, Set<String> gremlinAttributes, AtlasGraphQuery query) {
        if (criteria != null) {
            if (criteria.getCondition() != null) {
                if (criteria.getCondition() == Condition.AND) {
                    for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                        AtlasGraphQuery nestedQuery = toGremlinFilterQuery(type, filterCriteria, gremlinAttributes, context.getGraph().query());

                        query.addConditionsFrom(nestedQuery);
                    }
                } else {
                    List<AtlasGraphQuery> orConditions = new LinkedList<>();

                    for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                        AtlasGraphQuery nestedQuery = toGremlinFilterQuery(type, filterCriteria, gremlinAttributes, context.getGraph().query());

                        orConditions.add(context.getGraph().query().createChildQuery().addConditionsFrom(nestedQuery));
                    }

                    if (!orConditions.isEmpty()) {
                        query.or(orConditions);
                    }
                }
            } else if (gremlinAttributes.contains(criteria.getAttributeName())) {
                String                    attrName  = criteria.getAttributeName();
                String                    attrValue = criteria.getAttributeValue();
                SearchParameters.Operator operator  = criteria.getOperator();

                try {
                    final String qualifiedName = type.getQualifiedAttributeName(attrName);

                    switch (operator) {
                        case LT:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.LESS_THAN, attrValue);
                            break;
                        case LTE:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.LESS_THAN_EQUAL, attrValue);
                            break;
                        case GT:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.GREATER_THAN, attrValue);
                            break;
                        case GTE:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.GREATER_THAN_EQUAL, attrValue);
                            break;
                        case EQ:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.EQUAL, attrValue);
                            break;
                        case NEQ:
                            query.has(qualifiedName, AtlasGraphQuery.ComparisionOperator.NOT_EQUAL, attrValue);
                            break;
                        case LIKE:
                            // TODO: Maybe we need to validate pattern
                            query.has(qualifiedName, AtlasGraphQuery.MatchingOperator.REGEX, getLikeRegex(attrValue));
                            break;
                        case CONTAINS:
                            query.has(qualifiedName, AtlasGraphQuery.MatchingOperator.REGEX, getContainsRegex(attrValue));
                            break;
                        case STARTS_WITH:
                            query.has(qualifiedName, AtlasGraphQuery.MatchingOperator.PREFIX, attrValue);
                            break;
                        case ENDS_WITH:
                            query.has(qualifiedName, AtlasGraphQuery.MatchingOperator.REGEX, getSuffixRegex(attrValue));
                            break;
                        case IN:
                            LOG.warn("{}: unsupported operator. Ignored", operator);
                            break;
                    }
                } catch (AtlasBaseException e) {
                    LOG.error("toGremlinFilterQuery(): failed for attrName=" + attrName + "; operator=" + operator + "; attrValue=" + attrValue, e);
                }
            }
        }

        return query;
    }

    private String getContainsRegex(String attributeValue) {
        return ".*" + attributeValue + ".*";
    }

    private String getSuffixRegex(String attributeValue) {
        return ".*" + attributeValue;
    }

    private String getLikeRegex(String attributeValue) { return ".*" + attributeValue + ".*"; }

    protected List<AtlasVertex> getVerticesFromIndexQueryResult(Iterator<AtlasIndexQuery.Result> idxQueryResult) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (idxQueryResult != null) {
            while (idxQueryResult.hasNext()) {
                AtlasVertex vertex = idxQueryResult.next().getVertex();

                ret.add(vertex);
            }
        }

        return ret;
    }

    protected List<AtlasVertex> getVertices(Iterator<AtlasVertex> vertices) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (vertices != null) {
            while (vertices.hasNext()) {
                AtlasVertex vertex = vertices.next();

                ret.add(vertex);
            }
        }

        return ret;
    }

    protected Set<String> getGuids(List<AtlasVertex> vertices) {
        Set<String> ret = new HashSet<>();

        if (vertices != null) {
            for(AtlasVertex vertex : vertices) {
                String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);

                if (StringUtils.isNotEmpty(guid)) {
                    ret.add(guid);
                }
            }
        }

        return ret;
    }

    private static int getApplicationProperty(String propertyName, int defaultValue) {
        try {
            return ApplicationProperties.get().getInt(propertyName, defaultValue);
        } catch (AtlasException excp) {
            // ignore
        }

        return defaultValue;
    }

    private boolean hasOffendingChars(String str) {
        return StringUtils.containsAny(str, OFFENDING_CHARS);
    }
}
