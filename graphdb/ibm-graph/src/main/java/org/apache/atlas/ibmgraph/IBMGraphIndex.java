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

package org.apache.atlas.ibmgraph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.atlas.ibmgraph.api.json.Index;
import org.apache.atlas.ibmgraph.api.json.PropertyKey;
import org.apache.atlas.ibmgraph.api.json.Index.IndexType;
import org.apache.atlas.ibmgraph.util.GraphDBUtil;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Represents an index in IBM Graph.
 */
public class IBMGraphIndex implements AtlasGraphIndex {

    private IBMGraphManagement mgmt_;
    private Index wrapped_;


    public IBMGraphIndex(IBMGraphManagement mgmt,
            Index toWrap) {

        mgmt_ = mgmt;
        wrapped_ = toWrap;

    }

    @Override
    public boolean isEdgeIndex() {
        return wrapped_.getType() == IndexType.EDGE;
    }

    @Override
    public boolean isVertexIndex() {
        return wrapped_.getType() == IndexType.VERTEX;
    }

    @Override
    public boolean isUnique() {
        return wrapped_.isUnique();
    }


    @Override
    public Set<AtlasPropertyKey> getFieldKeys() {

        Set<AtlasPropertyKey> result = new HashSet<AtlasPropertyKey>();
        for(PropertyKey key : wrapped_.getPropertyKeys()) {
            result.add(GraphDBUtil.wrapPropertyKey(key));
        }
        return result;
    }

    public Collection<String> getFieldKeyNames() {

        return Collections2.transform(getFieldKeys(), new Function<AtlasPropertyKey,String>() {

            @Override
            public String apply(AtlasPropertyKey input) {
                return input.getName();
            }

        });
    }

    @Override
    public boolean isMixedIndex() {
        return ! wrapped_.isComposite();
    }

    @Override
    public boolean isCompositeIndex() {
        return wrapped_.isComposite();
    }

}
