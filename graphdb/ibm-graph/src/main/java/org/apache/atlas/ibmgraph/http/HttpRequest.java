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


public class HttpRequest {

    private URI uri_;
    private String json_;
    private RequestType requestType_;

    public URI getUri() {

        return uri_;
    }

    public void setUri(URI url) {

        uri_ = url;
    }

    public String getJson() {

        return json_;
    }

    public void setJson(String json) {

        json_ = json;
    }

    public RequestType getRequestType() {

        return requestType_;
    }

    public void setRequestType(RequestType requestType) {

        requestType_ = requestType;
    }

    public String toShortString() {
        StringBuffer reqStr = new StringBuffer();
        reqStr.append(requestType_);
        reqStr.append(' ').append(uri_);
        return reqStr.toString();

    }

    @Override
    public String toString() {

        StringBuffer reqStr = new StringBuffer();
        reqStr.append(requestType_);
        reqStr.append(' ').append(uri_);

        reqStr.append(System.getProperty("line.separator"));
        reqStr.append(json_);

        return reqStr.toString();
    }


}
