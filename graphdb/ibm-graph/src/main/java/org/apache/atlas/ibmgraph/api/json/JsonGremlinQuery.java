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

package org.apache.atlas.ibmgraph.api.json;

import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class JsonGremlinQuery {

    private String gremlin;
    private Map<String,Object> bindings = Collections.emptyMap();

    /**
     * @return the gremlin
     */
    public String getGremlin() {
        return gremlin;
    }

    /**
     * @param gremlin the gremlin to set
     */
    public void setGremlin(String gremlin) {
        this.gremlin = gremlin;
    }

    public Map<String, Object> getBindings() {
        return bindings;
    }

    public void setBindings(Map<String, Object> bindings) {
        if(bindings != null) {
            this.bindings = bindings;
        }
    }
}
