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
package org.apache.atlas.util;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.functors.NotPredicate;
import org.apache.commons.collections.functors.OrPredicate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;

public class SearchPredicateUtil {
    private static final Logger LOG = LoggerFactory.getLogger(SearchPredicateUtil.class);

    public static AttributeSearchPredicate getLTPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getLTPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            Object vertexAttrVal = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, attrClass);

                            if (vertexAttrVal == null) {
                                return false;
                            }

                            if (Long.class.isAssignableFrom(attrClass)) {
                                // Should cover Date comparison too
                                ret = ((Long) attrVal).compareTo((Long) vertexAttrVal) < 0;
                            } else if (Integer.class.isAssignableFrom(attrClass)) {
                                ret = ((Integer) attrVal).compareTo((Integer) vertexAttrVal) < 0;
                            } else if (Float.class.isAssignableFrom(attrClass)) {
                                ret = ((Float) attrVal).compareTo((Float) vertexAttrVal) < 0;
                            } else if (BigInteger.class.isAssignableFrom(attrClass)) {
                                ret = ((BigInteger) attrVal).compareTo((BigInteger) vertexAttrVal) < 0;
                            } else if (BigDecimal.class.isAssignableFrom(attrClass)) {
                                ret = ((BigDecimal) attrVal).compareTo((BigDecimal) vertexAttrVal) < 0;
                            } else if (Short.class.isAssignableFrom(attrClass)) {
                                ret = ((Short) attrVal).compareTo((Short) vertexAttrVal) < 0;
                            } else if (Double.class.isAssignableFrom(attrClass)) {
                                ret = ((Double) attrVal).compareTo((Double) vertexAttrVal) < 0;
                            } else if (Byte.class.isAssignableFrom(attrClass)) {
                                ret = ((Byte) attrVal).compareTo((Byte) vertexAttrVal) < 0;
                            }
                        }

                        return ret;
                    }
                };
            }
        };

        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getLTPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getGTPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getGTPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new NotPredicate(getLTEPredicate().generatePredicate(attrName, attrVal, attrClass));
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getGTPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getLTEPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getLTEPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new OrPredicate(
                        getLTPredicate().generatePredicate(attrName, attrVal, attrClass),
                        getEQPredicate().generatePredicate(attrName, attrVal, attrClass));
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getLTEPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getGTEPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getGTEPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new OrPredicate(
                        getGTPredicate().generatePredicate(attrName, attrVal, attrClass),
                        getEQPredicate().generatePredicate(attrName, attrVal, attrClass)
                );
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getGTEPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getEQPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getEQPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            Object vertexAttrVal = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, attrClass);

                            if (vertexAttrVal == null) {
                                return false;
                            }

                            if (BigInteger.class.isAssignableFrom(attrClass)) {
                                ret = ((BigInteger) attrVal).compareTo((BigInteger) vertexAttrVal) == 0;
                            } else if (BigDecimal.class.isAssignableFrom(attrClass)) {
                                ret = ((BigDecimal) attrVal).compareTo((BigDecimal) vertexAttrVal) == 0;
                            } else if (Number.class.isAssignableFrom(attrClass)) {
                                ret = Objects.equals(attrVal, vertexAttrVal);
                            } else if (String.class.isAssignableFrom(attrClass)) {
                                ret = StringUtils.equals((String) attrVal, (String) vertexAttrVal);
                            }
                        }

                        return ret;
                    }
                };
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getEQPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getNEQPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getNEQPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new NotPredicate(getEQPredicate().generatePredicate(attrName, attrVal, attrClass));
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getNEQPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getINPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getINPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            List vertexAttrValues = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, List.class);

                            if (vertexAttrValues == null) {
                                return false;
                            }

                            ret = vertexAttrValues.contains(attrVal);
                        }

                        return ret;
                    }
                };
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getINPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getLIKEPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getLIKEPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            Object vertexAttrVal = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, attrClass);

                            if (vertexAttrVal == null) {
                                return false;
                            }

                            if (String.class.isAssignableFrom(attrClass)) {
                                ret = StringUtils.contains((String) vertexAttrVal, (String) attrVal);
                            }
                        }

                        return ret;
                    }
                };
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getLIKEPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getStartsWithPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getStartsWithPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            Object vertexAttrVal = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, attrClass);

                            if (vertexAttrVal == null) {
                                return false;
                            }

                            if (String.class.isAssignableFrom(attrClass)) {
                                ret = StringUtils.startsWith((String) vertexAttrVal, (String) attrVal);
                            }
                        }

                        return ret;
                    }
                };
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getStartsWithPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getEndsWithPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getEndsWithPredicate");
        }
        AttributeSearchPredicate ret = new AttributeSearchPredicate() {
            @Override
            public Predicate generatePredicate(final String attrName, final Object attrVal, final Class attrClass) {
                return new Predicate() {
                    @Override
                    public boolean evaluate(final Object object) {
                        boolean ret = false;

                        if (attrVal == null) {
                            return false;
                        }

                        if (object instanceof AtlasVertex) {
                            Object vertexAttrVal = AtlasGraphUtilsV1.getProperty((AtlasVertex) object, attrName, attrClass);

                            if (vertexAttrVal == null) {
                                return false;
                            }

                            if (String.class.isAssignableFrom(attrClass)) {
                                ret = StringUtils.endsWith((String) vertexAttrVal, (String) attrVal);
                            }
                        }

                        return ret;
                    }
                };
            }
        };
        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getEndsWithPredicate");
        }
        return ret;
    }

    public static AttributeSearchPredicate getContainsPredicate() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("-> getContainsPredicate");
        }

        AttributeSearchPredicate ret =  getLIKEPredicate();

        if (LOG.isDebugEnabled()) {
            LOG.debug("<- getContainsPredicate");
        }
        return ret;
    }

    public interface AttributeSearchPredicate {
        Predicate generatePredicate(String attrName, Object attrVal, Class attrClass);
    }
}
