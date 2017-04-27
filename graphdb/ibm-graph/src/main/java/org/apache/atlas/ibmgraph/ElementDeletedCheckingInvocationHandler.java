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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.atlas.ibmgraph.util.AllowedWhenDeleted;
/**
 * InvocationHandler that checks whether the given element has been deleted and throws
 * an exception blocking it usage if that is the case.  Only methods with the {@code AllowedWhenDeleted}
 * annotation can be called on elements that have been deleted.  The annotations have been added to
 * match the behavior of Titan 1.0.0
 *
 */
public class ElementDeletedCheckingInvocationHandler implements InvocationHandler {

    private IBMGraphElement wrappedElement_;


    public ElementDeletedCheckingInvocationHandler(IBMGraphElement element) {
        wrappedElement_ = element;
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Method wrappedElementMethod = wrappedElement_.getClass().getMethod(method.getName(), method.getParameterTypes());

        if(wrappedElement_.isDeleted() && ! wrappedElementMethod.isAnnotationPresent(AllowedWhenDeleted.class)) {
            throw new IllegalStateException(wrappedElement_ + " has been deleted.");
        }

        try {
            return method.invoke(wrappedElement_, args);
        }
        catch(InvocationTargetException e) {
            throw e.getTargetException();
        }
    }


    public IBMGraphElement getWrappedElement() {
        return wrappedElement_;
    }
}