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

import org.apache.http.entity.ContentType;

/**
 * The response we get back from a http server
 */
public class HttpResponse {

    private int statusCode_;
    private String reasonPhrase_;
    private String responseBody_;
    private String contentType_;
    private String requestId_;


    public String getRequestId() {
        return requestId_;
    }
    public void setRequestId(String requestId) {
        requestId_ = requestId;
    }
    public int getStatusCode() {
        return statusCode_;
    }
    public void setStatusCode(int statusCode) {
        statusCode_ = statusCode;
    }
    public String getReasonPhrase() {
        return reasonPhrase_;
    }
    public void setReasonPhrase(String reasonPhrase) {
        reasonPhrase_ = reasonPhrase;
    }
    public String getResponseBody() {
        return responseBody_;
    }
    public void setResponseBody(String responseBody) {
        responseBody_ = responseBody;
    }
    public String getContentType() {
        return contentType_;
    }
    public void setContentType(String contentType) {
        contentType_ = contentType;
    }

    public boolean isJson() {
        if(getContentType() == null) {
            return false;
        }

        ContentType found = ContentType.parse(getContentType());
        return ContentType.APPLICATION_JSON.getMimeType().equals(found.getMimeType());

    }


    @Override
    public String toString() {

        StringBuffer reqStr = new StringBuffer();

        reqStr.append("HttpResponse[");
        reqStr.append("Status = ");
        reqStr.append(statusCode_);
        reqStr.append(" - ").append(reasonPhrase_);

        if(requestId_ != null) {
            reqStr.append(", IBM Graph Request Id = ");
            reqStr.append(requestId_);
        }
        reqStr.append("]");
        reqStr.append(System.getProperty("line.separator"));
        reqStr.append("Response Body:");
        reqStr.append(System.getProperty("line.separator"));
        reqStr.append(responseBody_);

        return reqStr.toString();
    }


}
