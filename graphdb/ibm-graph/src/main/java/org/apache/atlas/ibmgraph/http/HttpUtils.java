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

package org.apache.atlas.ibmgraph.http;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.atlas.ibmgraph.exception.GraphDatabaseException;

/**
 * Utility methods related to http
 */
public class HttpUtils {

    public static URI getApiUri(String uriString, String path) throws GraphDatabaseException {

        URI baseUri = null;
        try {
            baseUri = new URI(uriString);
        } catch (URISyntaxException ex) {
            throw new GraphDatabaseException("There was an error while creating URI for "
                    + uriString, ex);
        }

        StringBuilder uriBuilder = new StringBuilder();
        uriBuilder.append(uriString);
        if (!uriString.endsWith("/")) {
            uriBuilder.append('/');
        }

        uriBuilder.append(path);

        try {
            URI url = new URI(uriBuilder.toString());
            return url;

        } catch (URISyntaxException e) {
            throw new GraphDatabaseException("The request URI generated for the repository ("
                    + uriBuilder.toString() + ") is invalid.  Repository: " + baseUri, e);
        }
    }

}
