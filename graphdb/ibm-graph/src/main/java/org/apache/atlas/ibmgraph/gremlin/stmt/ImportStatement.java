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

package org.apache.atlas.ibmgraph.gremlin.stmt;

import java.util.Collections;
import java.util.List;

import org.apache.atlas.groovy.AbstractGroovyExpression;
import org.apache.atlas.groovy.GroovyExpression;
import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Gremlin statement that represents a for loop
 */
public class ImportStatement extends AbstractGroovyExpression {

    private String class_;
    private boolean isStatic_;


    public ImportStatement(String clazz) {
        super();
        class_ = clazz;
        isStatic_  = false;
    }

    public ImportStatement(boolean isStatic, String clazz) {
        super();
        class_ = clazz;
        isStatic_  = isStatic;
    }

    public void generateGroovy(GroovyGenerationContext context) {

        context.append("import ");
        if(isStatic_) {
            context.append("static ");
        }
        context.append(class_);
    }

    @Override
    public List<GroovyExpression> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public GroovyExpression copy(List<GroovyExpression> newChildren) {
        return new ImportStatement(isStatic_, class_);
    }

}
