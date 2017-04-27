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

import java.util.List;

import com.google.gson.JsonElement;

/**
 *
 */
public class JsonResponse {

    /**
     * @return the requestId
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * @param requestId the requestId to set
     */
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * @return the result
     */
    public Result getResult() {
        return result;
    }

    /**
     * @param result the result to set
     */
    public void setResult(Result result) {
        this.result = result;
    }

    private String requestId;
    private Status status;
    private Result result;

    public static class Result {
        List<JsonElement> data;

        /**
         * @return the data
         */
        public List<JsonElement> getData() {
            return data;
        }

        /**
         * @param data the data to set
         */
        public void setData(List<JsonElement> data) {
            this.data = data;
        }


    }

    public static class Status {

        private String message;
        private int code;
        private Object attributes;
        /**
         * @return the message
         */
        public String getMessage() {
            return message;
        }
        /**
         * @param message the message to set
         */
        public void setMessage(String message) {
            this.message = message;
        }
        /**
         * @return the code
         */
        public int getCode() {
            return code;
        }
        /**
         * @param code the code to set
         */
        public void setCode(int code) {
            this.code = code;
        }
        /**
         * @return the attributes
         */
        public Object getAttributes() {
            return attributes;
        }
        /**
         * @param attributes the attributes to set
         */
        public void setAttributes(Object attributes) {
            this.attributes = attributes;
        }

        public JsonErrorResponse toErrorResponse() {
            JsonErrorResponse response = new JsonErrorResponse();
            response.setCode(String.valueOf(getCode()));
            response.setMessage(getMessage());
            return response;
        }


    }
}
