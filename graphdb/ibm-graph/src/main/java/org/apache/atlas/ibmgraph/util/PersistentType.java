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

import java.util.function.Function;

import org.apache.atlas.groovy.CastExpression;
import org.apache.atlas.groovy.FunctionCallExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.IdentifierExpression;
import org.apache.atlas.groovy.LiteralExpression;
import org.apache.atlas.ibmgraph.IBMGraphElement;
import org.apache.atlas.typesystem.types.DataTypes.ArrayType;
import org.apache.atlas.typesystem.types.DataTypes.BooleanType;
import org.apache.atlas.typesystem.types.DataTypes.ByteType;
import org.apache.atlas.typesystem.types.DataTypes.DateType;
import org.apache.atlas.typesystem.types.DataTypes.DoubleType;
import org.apache.atlas.typesystem.types.DataTypes.FloatType;
import org.apache.atlas.typesystem.types.DataTypes.IntType;
import org.apache.atlas.typesystem.types.DataTypes.LongType;
import org.apache.atlas.typesystem.types.DataTypes.ShortType;
import org.apache.atlas.typesystem.types.DataTypes.StringType;
import org.apache.atlas.typesystem.types.IDataType;

public enum PersistentType {


    BOOLEAN(false, new CastFunction("Boolean")),
    BYTE(false, new CastFunction("Byte")),
    DATE(true, new ValueOfFunction("Long")),
    DOUBLE(true, new ValueOfFunction("Double")),
    FLOAT(false, new CastFunction("Float")),
    INTEGER(false, new CastFunction("Integer")),
    LONG(true, new ValueOfFunction("Long")),
    SHORT(false, new CastFunction("Short")),
    STRING(false, new CastFunction("String")),
    LIST(true, new Function<GroovyExpression,GroovyExpression>() {

        @Override
        public GroovyExpression apply(GroovyExpression t) {
            GroovyExpression stringValue = new CastExpression(t, "String");
            GroovyExpression stringValueWithoutListPrefix = new FunctionCallExpression(stringValue,"substring", LIST_PREFIX_LENGTH_EXPR);
            GroovyExpression listExpression = new FunctionCallExpression(stringValueWithoutListPrefix, "split", LIST_DELIMITER_EXPR);
            return listExpression;
        }
    }),
    UNKNOWN(false, Function.identity());

    private final Function<GroovyExpression,GroovyExpression> conversionFunction_;
    private final boolean conversionRequired_;

    private static final GroovyExpression LIST_PREFIX_LENGTH_EXPR = new LiteralExpression(IBMGraphElement.LIST_PREFIX.length());
    private static final GroovyExpression LIST_DELIMITER_EXPR = new LiteralExpression(IBMGraphElement.LIST_DELIMITER);



    private PersistentType(boolean conversionRequired,
                           Function<GroovyExpression,GroovyExpression> conversionFunction) {

        conversionFunction_ = conversionFunction;
        conversionRequired_ = conversionRequired;
    }

    public GroovyExpression generateConversionExpression(GroovyExpression expr) {

        return conversionFunction_.apply(expr);
    }

    public static PersistentType valueOf(IDataType<?> type) {

        if(type instanceof BooleanType) { return BOOLEAN; }
        if(type instanceof DateType) { return DATE; }
        if(type instanceof StringType) { return STRING; }

        if(type instanceof ByteType) {return BYTE; }
        if(type instanceof ShortType) {return SHORT; }
        if(type instanceof IntType) {return INTEGER; }
        if(type instanceof LongType) { return LONG; }

        if(type instanceof FloatType) { return FLOAT; }
        if(type instanceof DoubleType) { return DOUBLE; }
        if(type instanceof ArrayType) { return LIST; }
        return UNKNOWN;

    }

    public boolean isPropertyValueConversionNeeded() {
        return conversionRequired_;
    }

    /**
     * Conversion function that casts the expression to a given type.
     *
     */
    private static class CastFunction implements Function<GroovyExpression,GroovyExpression> {
        private final String type_;
        public CastFunction(String type) {
            type_ = type;
        }
        @Override
        public GroovyExpression apply(GroovyExpression toConvert) {
            return new CastExpression(toConvert, type_);
        }
    }

    /**
     * Conversion function that calls a static "valueOf" function (such as Long.valueOf(..))
     * to do the conversion.
     */
    private static class ValueOfFunction implements Function<GroovyExpression,GroovyExpression> {

        private static final String VALUE_OF = "valueOf";
        private final IdentifierExpression type_;
        private static final IdentifierExpression STRING = new IdentifierExpression("String");
        public ValueOfFunction(String type) {
            type_ = new IdentifierExpression(type);
        }
        @Override
        public GroovyExpression apply(GroovyExpression toConvert) {
            GroovyExpression stringValue = new FunctionCallExpression(STRING, VALUE_OF, toConvert);
            return new FunctionCallExpression(type_, VALUE_OF, stringValue);
        }
    }
}