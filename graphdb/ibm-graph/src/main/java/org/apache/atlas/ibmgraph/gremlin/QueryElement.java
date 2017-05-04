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

package org.apache.atlas.ibmgraph.gremlin;

import java.util.Iterator;
import java.util.List;

import org.apache.atlas.groovy.GroovyGenerationContext;

/**
 * Base class for things that can be translated into Gremlin.
 */
public abstract class QueryElement {


    public abstract void generateGroovy(GroovyGenerationContext context);

    protected void appendSingleQuotedString(StringBuilder builder, String str) {
        builder.append("'");
        builder.append(str);
        builder.append("'");
    }

    protected void appendSingleQuotedStringList(StringBuilder builder, List<String> values) {
        if(values != null) {
            Iterator<String> it = values.iterator();
            while(it.hasNext()) {
                String s = it.next();
                appendSingleQuotedString(builder, s);
                if(it.hasNext()) {
                    builder.append(",");
                }
            }
        }
    }

}
