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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Utility methods for manipulating files
 *
 *
 */
public class FileUtils {

    /**
     * Reads everything from the specified input stream and
     * returns a String with the content.
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static String readStream(InputStream in) throws IOException {

        BufferedReader input = new BufferedReader(new InputStreamReader(in));
        return readStream(input);
    }

    /**
     * Reads everything from the specified reader and
     * returns a String with the content.
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static String readStream(Reader reader) throws IOException {
        try {
            StringBuffer buffer = new StringBuffer();
            BufferedReader input = new BufferedReader(reader);

            char[] bytes = new char[2048];
            int read;
            while ((read = input.read(bytes)) > 0) {
                buffer.append(bytes, 0, read);
            }
            return buffer.toString();
        }
        finally {
            reader.close();
        }
    }
}
