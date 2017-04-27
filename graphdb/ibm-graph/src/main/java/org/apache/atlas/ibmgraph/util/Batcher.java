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

import java.util.Iterator;
import java.util.List;

/**
 * Batcher that splits a list into batches with at
 * most batchSize elements each.
 *
 * @param <T>
 */
public class Batcher<T> implements Iterator<List<T>> {

       private int nextBatchNumber = 0;
       private final int nBatches;
       private final List<T> allItems;
       private final int batchSize;

       public Batcher(List<T> itemsToBatch, int batchSize) {
           nextBatchNumber = 0;
           this.allItems = itemsToBatch;

           if(batchSize > 0) {
               nBatches = (int)Math.ceil((double)itemsToBatch.size() / batchSize);
               this.batchSize = batchSize;
           }
           else {
               //one batch
               nBatches = 1;
              this. batchSize = itemsToBatch.size();
           }
       }

        @Override
        public boolean hasNext() {
            return nextBatchNumber < nBatches;
        }

        @Override
        public List<T> next() {
            int startIndexIncl = batchSize*nextBatchNumber;
            int endIndexExcl = Integer.min(batchSize*(nextBatchNumber+1), allItems.size());
            nextBatchNumber++;
            return allItems.subList(startIndexIncl, endIndexExcl);
        }

        public int getBatchCount() {
            return nBatches;
        }
   }