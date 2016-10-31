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
package org.apache.atlas.store;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;

import java.util.List;

import static org.apache.atlas.model.typedef.AtlasEnumDef.*;

/**
 * Interface to persistence store of TypeDef
 */
public interface AtlasTypeDefStore {
    void init() throws AtlasBaseException;

    /***********************/
    /** EnumDef operation **/
    /***********************/

    /**
     * Create the given enum definition
     * @param enumDef enum definition
     * @return Created enum definition (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException;

    /**
     * Get all the enums
     * @return List of defined enums
     * @throws AtlasBaseException
     */
    List<AtlasEnumDef> getAllEnumDefs() throws AtlasBaseException;

    /**
     * Get the enum def by it's name
     * @param name unique name identifier
     * @return Enum def
     * @throws AtlasBaseException
     */
    AtlasEnumDef getEnumDefByName(String name) throws AtlasBaseException;

    /**
     * Get the enum def by it's GUID
     * @param guid unique identifier
     * @return Enum def
     * @throws AtlasBaseException
     */
    AtlasEnumDef getEnumDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Update the given enum def
     * @param name unique name identifier
     * @param enumDef Enum definition
     * @return Updated enum def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEnumDef updateEnumDefByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException;

    /**
     * Update the given enum def
     * @param guid Unique identifier
     * @param enumDef Enum definition
     * @return Update enum def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException;

    /**
     * Delete the enum identified by given name
     * @param name unique name
     * @throws AtlasBaseException
     */
    void deleteEnumDefByName(String name) throws AtlasBaseException;

    /**
     * Delete the enum identified by given guid
     * @param guid Unique identifier
     * @throws AtlasBaseException
     */
    void deleteEnumDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Search for enum defs that match the given search criteria ({@see SearchFilter})
     * @param filter Search filter
     * @return Enum defs satisfying the search criteria
     * @throws AtlasBaseException
     */
    AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException;

    /*************************/
    /** StructDef operation **/
    /*************************/

    /**
     * Create the given struct definition
     * @param structDef struct definition
     * @return Created struct definition (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException;

    /**
     * Get all the Struct definitions
     * @return List of defined struct
     * @throws AtlasBaseException
     */
    List<AtlasStructDef> getAllStructDefs() throws AtlasBaseException;

    /**
     * Get the struct def by it's name
     * @param name unique name identifier
     * @return Struct def
     * @throws AtlasBaseException
     */
    AtlasStructDef getStructDefByName(String name) throws AtlasBaseException;

    /**
     * Get the Struct def by it's GUID
     * @param guid unique identifier
     * @return Struct def
     * @throws AtlasBaseException
     */
    AtlasStructDef getStructDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Update the given struct def
     * @param name unique name identifier
     * @param structDef struct definition
     * @return Updated struct def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasStructDef updateStructDefByName(String name, AtlasStructDef structDef) throws AtlasBaseException;

    /**
     * Update the given Struct def
     * @param guid Unique identifier
     * @param structDef Struct definition
     * @return Update Struct def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException;

    /**
     * Delete the struct identified by given name
     * @param name unique name
     * @throws AtlasBaseException
     */
    void deleteStructDefByName(String name) throws AtlasBaseException;

    /**
     * Delete the struct identified by given guid
     * @param guid Unique identifier
     * @throws AtlasBaseException
     */
    void deleteStructDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Search for strcut defs that match the given search criteria ({@see SearchFilter})
     * @param filter Search filter
     * @return Struct defs satisfying the search criteria
     * @throws AtlasBaseException
     */
    AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException;


    /*********************************/
    /** ClassificationDef operation **/
    /*********************************/

    /**
     * Create the given classification definition
     * @param classificationDef classification definition
     * @return Created classification definition (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef) throws AtlasBaseException;

    /**
     * Get all the Struct definitions
     * @return List of defined struct
     * @throws AtlasBaseException
     */
    List<AtlasClassificationDef> getAllClassificationDefs() throws AtlasBaseException;

    /**
     * Get the classification def by it's name
     * @param name unique name identifier
     * @return Classification def
     * @throws AtlasBaseException
     */
    AtlasClassificationDef getClassificationDefByName(String name) throws AtlasBaseException;

    /**
     * Get the Classification def by it's GUID
     * @param guid unique identifier
     * @return Classification def
     * @throws AtlasBaseException
     */
    AtlasClassificationDef getClassificationDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Update the given Classification def
     * @param name unique name identifier
     * @param classificationDef Classification definition
     * @return Updated Classification def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasClassificationDef updateClassificationDefByName(String name, AtlasClassificationDef classificationDef)
            throws AtlasBaseException;

    /**
     * Update the given Classification def
     * @param guid Unique identifier
     * @param classificationDef Classification definition
     * @return Update Classification def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef)
            throws AtlasBaseException;

    /**
     * Delete the classification identified by given name
     * @param name unique name
     * @throws AtlasBaseException
     */
    void deleteClassificationDefByName(String name) throws AtlasBaseException;

    /**
     * Delete the classification identified by given guid
     * @param guid Unique identifier
     * @throws AtlasBaseException
     */
    void deleteClassificationDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Search for classification defs that match the given search criteria ({@see SearchFilter})
     * @param filter Search filter
     * @return Classification defs satisfying the search criteria
     * @throws AtlasBaseException
     */
    AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException;


    /*************************/
    /** EntityDef operation **/
    /*************************/

    /**
     * Create the given entity definition
     * @param entityDef entity definition
     * @return Created entity definition (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException;

    /**
     * Get all the Entity definitions
     * @return List of defined entities
     * @throws AtlasBaseException
     */
    List<AtlasEntityDef> getAllEntityDefs() throws AtlasBaseException;

    /**
     * Get the entity def by it's name
     * @param name unique name identifier
     * @return Entity def
     * @throws AtlasBaseException
     */
    AtlasEntityDef getEntityDefByName(String name) throws AtlasBaseException;

    /**
     * Get the Entity def by it's GUID
     * @param guid unique identifier
     * @return Entity def
     * @throws AtlasBaseException
     */
    AtlasEntityDef getEntityDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Update the given Entity def
     * @param name unique name identifier
     * @param entityDef Entity definition
     * @return Updated Entity def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEntityDef updateEntityDefByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException;

    /**
     * Update the given entity def
     * @param guid Unique identifier
     * @param entityDef entity definition
     * @return Update entity def (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException;

    /**
     * Delete the entity identified by given name
     * @param name unique name
     * @throws AtlasBaseException
     */
    void deleteEntityDefByName(String name) throws AtlasBaseException;

    /**
     * Delete the entity identified by given guid
     * @param guid Unique identifier
     * @throws AtlasBaseException
     */
    void deleteEntityDefByGuid(String guid) throws AtlasBaseException;

    /**
     * Search for entity defs that match the given search criteria ({@see SearchFilter})
     * @param filter Search filter
     * @return Entity defs satisfying the search criteria
     * @throws AtlasBaseException
     */
    AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException;

    /***** Bulk Operations *****/

    /**
     * Create the given type definition(s)
     * @param atlasTypesDef type definition(s)
     * @return Created type definition(s) (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasTypesDef createTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    /**
     * Update the given type definition(s)
     * @param atlasTypesDef type definition(s)
     * @return Updated type definition(s) (missing values would be set to the default one)
     * @throws AtlasBaseException
     */
    AtlasTypesDef updateTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    /**
     * Delete the given type definition(s)
     * @param atlasTypesDef type definition(s)
     * @throws AtlasBaseException
     */
    void deleteTypesDef(AtlasTypesDef atlasTypesDef) throws AtlasBaseException;

    /**
     * Create the given type definition(s)
     * @param searchFilter Search criteria
     * @return Type definition(s) that match the given search criteria or an empty list if nothing
     * matches
     * @throws AtlasBaseException
     */
    AtlasTypesDef searchTypesDef(SearchFilter searchFilter) throws AtlasBaseException;
}
