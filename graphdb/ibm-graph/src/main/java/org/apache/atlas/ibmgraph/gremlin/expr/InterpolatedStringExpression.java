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

package org.apache.atlas.ibmgraph.gremlin.expr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Class that represents an interpolated String.  For example:
 * "Hello ${name}!", where name is a variable.  In general,
 * any valid gremlin groovy expression can go inside the ${}.
 *
 * @author jeff
 *
 */
public class InterpolatedStringExpression extends AbstractGroovyExpression {

    private List<StringLiteralSection> parts_ = new ArrayList<>();

    /**
     * Adds a literal String value.
     *
     * @param s
     */
    public void append(String s) {
        parts_.add(new StringSection(s));
    }

    /**
     * Adds an inline function into the String.  It will be
     * surrounded by ${...} when it is translated, and embedded
     * into the String.
     *
     * @param expr
     */
    public void append(GroovyExpression expr) {
        parts_.add(new InlineExpression(expr));
    }

    private static abstract class StringLiteralSection extends AbstractGroovyExpression {

    }

    private static class StringSection extends StringLiteralSection {

        private String value_;

        public StringSection(String value) {
            value_ = value;
        }

        @Override
        public void generateGroovy(GroovyGenerationContext context) {
            context.append(value_);
        }

        @Override
        public List<GroovyExpression> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public GroovyExpression copy(List<GroovyExpression> newChildren) {
            return new StringSection(value_);
        }
    }

    private static class InlineExpression extends StringLiteralSection {

        private GroovyExpression toInterpolate_;

        public InlineExpression(GroovyExpression toInterpolate) {
            toInterpolate_ = toInterpolate;
        }

        @Override
        public void generateGroovy(GroovyGenerationContext context) {
            context.append("${");
            toInterpolate_.generateGroovy(context);
            context.append("}");
        }

        @Override
        public List<GroovyExpression> getChildren() {
            return Collections.singletonList(toInterpolate_);
        }

        @Override
        public GroovyExpression copy(List<GroovyExpression> newChildren) {
            assert newChildren.size() == 1;
            return new InlineExpression(newChildren.get(0));
        }
    }

    @Override
    public void generateGroovy(GroovyGenerationContext context) {
        //double quotes are needed because string interpolation does not happen when using single quotes, and the syntax above
        //relies on string interpolation to get the element ids into the string.  We do this because '+' and the String functions
        //we would otherwise use are not white listed.  This syntax is part of the Groovy programming language and cannot be blocked.
        context.append("\"");
        for(StringLiteralSection section : parts_) {
            section.generateGroovy(context);
        }
        context.append("\"");
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.unmodifiableList(parts_);
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        assert newChildren.size() == parts_.size();
        InterpolatedStringExpression result = new InterpolatedStringExpression();
        for(GroovyExpression expr : newChildren) {
            result.parts_.add((StringLiteralSection)expr);
        }
        return result;
    }
}
