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
package org.apache.atlas.repository.store.graph;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.listener.ChangedTypeDefs;
import org.apache.atlas.listener.TypeDefChangeListener;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef.AtlasClassificationDefs;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEntityDef.AtlasEntityDefs;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumDefs;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeRegistry.AtlasTransientTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.Transformer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;


/**
 * Abstract class for graph persistence store for TypeDef
 */
public abstract class AtlasTypeDefGraphStore implements AtlasTypeDefStore, ActiveStateChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasTypeDefGraphStore.class);

    private final AtlasTypeRegistry          typeRegistry;
    private final Set<TypeDefChangeListener> typeDefChangeListeners;
    private final int                        typeUpdateLockMaxWaitTimeSeconds;

    protected AtlasTypeDefGraphStore(AtlasTypeRegistry typeRegistry,
                                     Set<TypeDefChangeListener> typeDefChangeListeners) {
        this.typeRegistry                     = typeRegistry;
        this.typeDefChangeListeners           = typeDefChangeListeners;
        this.typeUpdateLockMaxWaitTimeSeconds = AtlasRepositoryConfiguration.getTypeUpdateLockMaxWaitTimeInSeconds();
    }

    protected abstract AtlasEnumDefStore getEnumDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasStructDefStore getStructDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasClassificationDefStore getClassificationDefStore(AtlasTypeRegistry typeRegistry);

    protected abstract AtlasEntityDefStore getEntityDefStore(AtlasTypeRegistry typeRegistry);

    @Override
    public void init() throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr           = null;
        boolean                    commitUpdates = false;

        try {
            ttr = typeRegistry.lockTypeRegistryForUpdate(typeUpdateLockMaxWaitTimeSeconds);

            AtlasTypesDef typesDef = new AtlasTypesDef(getEnumDefStore(ttr).getAll(),
                    getStructDefStore(ttr).getAll(),
                    getClassificationDefStore(ttr).getAll(),
                    getEntityDefStore(ttr).getAll());

            ttr.addTypes(typesDef);

            commitUpdates = true;
        } finally {
            typeRegistry.releaseTypeRegistryForUpdate(ttr, commitUpdates);
        }

        bootstrapTypes();
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef createEnumDef(AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addType(enumDef);

        AtlasEnumDef ret = getEnumDefStore(ttr).create(enumDef);

        ttr.updateGuid(ret.getName(), ret.getGuid());

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEnumDef> getAllEnumDefs() throws AtlasBaseException {
        Collection<AtlasEnumDef> enumDefs = typeRegistry.getAllEnumDefs();

        return CollectionUtils.isNotEmpty(enumDefs) ? new ArrayList<>(enumDefs) : Collections.<AtlasEnumDef>emptyList();
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByName(String name) throws AtlasBaseException {
        AtlasEnumDef ret = typeRegistry.getEnumDefByName(name);
        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }
        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef getEnumDefByGuid(String guid) throws AtlasBaseException {
        AtlasEnumDef ret = typeRegistry.getEnumDefByGuid(guid);
        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }
        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByName(name, enumDef);

        return getEnumDefStore(ttr).updateByName(name, enumDef);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDef updateEnumDefByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByGuid(guid, enumDef);

        return getEnumDefStore(ttr).updateByGuid(guid, enumDef);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByName(name);

        getEnumDefStore(ttr).deleteByName(name);
    }

    @Override
    @GraphTransaction
    public void deleteEnumDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByGuid(guid);

        getEnumDefStore(ttr).deleteByGuid(guid);
    }

    @Override
    @GraphTransaction
    public AtlasEnumDefs searchEnumDefs(SearchFilter filter) throws AtlasBaseException {
        return getEnumDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef createStructDef(AtlasStructDef structDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addType(structDef);

        AtlasStructDef ret = getStructDefStore(ttr).create(structDef, null);

        ttr.updateGuid(ret.getName(), ret.getGuid());

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasStructDef> getAllStructDefs() throws AtlasBaseException {
        Collection<AtlasStructDef> structDefs = typeRegistry.getAllStructDefs();

        return CollectionUtils.isNotEmpty(structDefs) ? new ArrayList<>(structDefs) : Collections.<AtlasStructDef>emptyList();
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByName(String name) throws AtlasBaseException {
        AtlasStructDef ret = typeRegistry.getStructDefByName(name);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef getStructDefByGuid(String guid) throws AtlasBaseException {
        AtlasStructDef ret = typeRegistry.getStructDefByGuid(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByName(String name, AtlasStructDef structDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByName(name, structDef);

        return getStructDefStore(ttr).updateByName(name, structDef);
    }

    @Override
    @GraphTransaction
    public AtlasStructDef updateStructDefByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByGuid(guid, structDef);

        return getStructDefStore(ttr).updateByGuid(guid, structDef);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByName(name);

        getStructDefStore(ttr).deleteByName(name, null);
    }

    @Override
    @GraphTransaction
    public void deleteStructDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByGuid(guid);

        getStructDefStore(ttr).deleteByGuid(guid, null);
    }

    @Override
    @GraphTransaction
    public AtlasStructDefs searchStructDefs(SearchFilter filter) throws AtlasBaseException {
        return getStructDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef createClassificationDef(AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addType(classificationDef);

        AtlasClassificationDef ret = getClassificationDefStore(ttr).create(classificationDef, null);

        ttr.updateGuid(ret.getName(), ret.getGuid());

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasClassificationDef> getAllClassificationDefs() throws AtlasBaseException {
        Collection<AtlasClassificationDef> classificationDefs = typeRegistry.getAllClassificationDefs();

        return CollectionUtils.isNotEmpty(classificationDefs) ? new ArrayList<>(classificationDefs)
                                                              : Collections.<AtlasClassificationDef>emptyList();
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByName(String name) throws AtlasBaseException {
        AtlasClassificationDef ret = typeRegistry.getClassificationDefByName(name);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef getClassificationDefByGuid(String guid) throws AtlasBaseException {
        AtlasClassificationDef ret = typeRegistry.getClassificationDefByGuid(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByName(String name, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByName(name, classificationDef);

        return getClassificationDefStore(ttr).updateByName(name, classificationDef);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDef updateClassificationDefByGuid(String guid, AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByGuid(guid, classificationDef);

        return getClassificationDefStore(ttr).updateByGuid(guid, classificationDef);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByName(name);

        getClassificationDefStore(ttr).deleteByName(name, null);
    }

    @Override
    @GraphTransaction
    public void deleteClassificationDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByGuid(guid);

        getClassificationDefStore(ttr).deleteByGuid(guid, null);
    }

    @Override
    @GraphTransaction
    public AtlasClassificationDefs searchClassificationDefs(SearchFilter filter) throws AtlasBaseException {
        return getClassificationDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef createEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addType(entityDef);

        AtlasEntityDef ret = getEntityDefStore(ttr).create(entityDef, null);

        ttr.updateGuid(ret.getName(), ret.getGuid());

        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasEntityDef> getAllEntityDefs() throws AtlasBaseException {
        Collection<AtlasEntityDef> entityDefs = typeRegistry.getAllEntityDefs();

        return CollectionUtils.isNotEmpty(entityDefs) ? new ArrayList<>(entityDefs) : Collections.<AtlasEntityDef>emptyList();
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByName(String name) throws AtlasBaseException {
        AtlasEntityDef ret = typeRegistry.getEntityDefByName(name);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef getEntityDefByGuid(String guid) throws AtlasBaseException {
        AtlasEntityDef ret = typeRegistry.getEntityDefByGuid(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByName(String name, AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByName(name, entityDef);

        return getEntityDefStore(ttr).updateByName(name, entityDef);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDef updateEntityDefByGuid(String guid, AtlasEntityDef entityDef) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypeByGuid(guid, entityDef);

        return getEntityDefStore(ttr).updateByGuid(guid, entityDef);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByName(String name) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByName(name);

        getEntityDefStore(ttr).deleteByName(name, null);
    }

    @Override
    @GraphTransaction
    public void deleteEntityDefByGuid(String guid) throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.removeTypeByGuid(guid);

        getEntityDefStore(ttr).deleteByGuid(guid, null);
    }

    @Override
    @GraphTransaction
    public AtlasEntityDefs searchEntityDefs(SearchFilter filter) throws AtlasBaseException {
        return getEntityDefStore(typeRegistry).search(filter);
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef createTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeDefGraphStore.createTypesDef(enums={}, structs={}, classfications={}, entities={})",
                      CollectionUtils.size(typesDef.getEnumDefs()),
                      CollectionUtils.size(typesDef.getStructDefs()),
                      CollectionUtils.size(typesDef.getClassificationDefs()),
                      CollectionUtils.size(typesDef.getEntityDefs()));
        }

        AtlasTypesDef ret = new AtlasTypesDef();

        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addTypes(typesDef);

        AtlasEnumDefStore           enumDefStore     = getEnumDefStore(ttr);
        AtlasStructDefStore         structDefStore   = getStructDefStore(ttr);
        AtlasClassificationDefStore classifiDefStore = getClassificationDefStore(ttr);
        AtlasEntityDefStore         entityDefStore   = getEntityDefStore(ttr);

        List<Object> preCreateStructDefs   = new ArrayList<>();
        List<Object> preCreateClassifiDefs = new ArrayList<>();
        List<Object> preCreateEntityDefs   = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
                AtlasEnumDef createdDef = enumDefStore.create(enumDef);

                ttr.updateGuid(createdDef.getName(), createdDef.getGuid());

                ret.getEnumDefs().add(createdDef);
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                preCreateStructDefs.add(structDefStore.preCreate(structDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef classifiDef : typesDef.getClassificationDefs()) {
                preCreateClassifiDefs.add(classifiDefStore.preCreate(classifiDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                preCreateEntityDefs.add(entityDefStore.preCreate(entityDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            int i = 0;
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                AtlasStructDef createdDef = structDefStore.create(structDef, preCreateStructDefs.get(i));

                ttr.updateGuid(createdDef.getName(), createdDef.getGuid());

                ret.getStructDefs().add(createdDef);
                i++;
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            int i = 0;
            for (AtlasClassificationDef classifiDef : typesDef.getClassificationDefs()) {
                AtlasClassificationDef createdDef = classifiDefStore.create(classifiDef, preCreateClassifiDefs.get(i));

                ttr.updateGuid(createdDef.getName(), createdDef.getGuid());

                ret.getClassificationDefs().add(createdDef);
                i++;
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            int i = 0;
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                AtlasEntityDef createdDef = entityDefStore.create(entityDef, preCreateEntityDefs.get(i));

                ttr.updateGuid(createdDef.getName(), createdDef.getGuid());

                ret.getEntityDefs().add(createdDef);
                i++;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeDefGraphStore.createTypesDef(enums={}, structs={}, classfications={}, entities={})",
                    CollectionUtils.size(typesDef.getEnumDefs()),
                    CollectionUtils.size(typesDef.getStructDefs()),
                    CollectionUtils.size(typesDef.getClassificationDefs()),
                    CollectionUtils.size(typesDef.getEntityDefs()));
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef updateTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeDefGraphStore.updateTypesDef(enums={}, structs={}, classfications={}, entities={})",
                    CollectionUtils.size(typesDef.getEnumDefs()),
                    CollectionUtils.size(typesDef.getStructDefs()),
                    CollectionUtils.size(typesDef.getClassificationDefs()),
                    CollectionUtils.size(typesDef.getEntityDefs()));
        }

        AtlasTypesDef ret = new AtlasTypesDef();

        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.updateTypes(typesDef);

        AtlasEnumDefStore           enumDefStore     = getEnumDefStore(ttr);
        AtlasStructDefStore         structDefStore   = getStructDefStore(ttr);
        AtlasClassificationDefStore classifiDefStore = getClassificationDefStore(ttr);
        AtlasEntityDefStore         entityDefStore   = getEntityDefStore(ttr);

        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
                ret.getEnumDefs().add(enumDefStore.update(enumDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                ret.getStructDefs().add(structDefStore.update(structDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef classifiDef : typesDef.getClassificationDefs()) {
                ret.getClassificationDefs().add(classifiDefStore.update(classifiDef));
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                ret.getEntityDefs().add(entityDefStore.update(entityDef));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeDefGraphStore.updateTypesDef(enums={}, structs={}, classfications={}, entities={})",
                    CollectionUtils.size(typesDef.getEnumDefs()),
                    CollectionUtils.size(typesDef.getStructDefs()),
                    CollectionUtils.size(typesDef.getClassificationDefs()),
                    CollectionUtils.size(typesDef.getEntityDefs()));
        }

        return ret;

    }

    @Override
    @GraphTransaction
    public void deleteTypesDef(AtlasTypesDef typesDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeDefGraphStore.deleteTypesDef(enums={}, structs={}, classfications={}, entities={})",
                    CollectionUtils.size(typesDef.getEnumDefs()),
                    CollectionUtils.size(typesDef.getStructDefs()),
                    CollectionUtils.size(typesDef.getClassificationDefs()),
                    CollectionUtils.size(typesDef.getEntityDefs()));
        }

        AtlasTransientTypeRegistry ttr = lockTypeRegistryAndReleasePostCommit();

        ttr.addTypes(typesDef);

        AtlasEnumDefStore           enumDefStore     = getEnumDefStore(ttr);
        AtlasStructDefStore         structDefStore   = getStructDefStore(ttr);
        AtlasClassificationDefStore classifiDefStore = getClassificationDefStore(ttr);
        AtlasEntityDefStore         entityDefStore   = getEntityDefStore(ttr);

        List<Object> preDeleteStructDefs   = new ArrayList<>();
        List<Object> preDeleteClassifiDefs = new ArrayList<>();
        List<Object> preDeleteEntityDefs   = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                if (StringUtils.isNotBlank(structDef.getGuid())) {
                    preDeleteStructDefs.add(structDefStore.preDeleteByGuid(structDef.getGuid()));
                } else {
                    preDeleteStructDefs.add(structDefStore.preDeleteByName(structDef.getName()));
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            for (AtlasClassificationDef classifiDef : typesDef.getClassificationDefs()) {
                if (StringUtils.isNotBlank(classifiDef.getGuid())) {
                    preDeleteClassifiDefs.add(classifiDefStore.preDeleteByGuid(classifiDef.getGuid()));
                } else {
                    preDeleteClassifiDefs.add(classifiDefStore.preDeleteByName(classifiDef.getName()));
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                if (StringUtils.isNotBlank(entityDef.getGuid())) {
                    preDeleteEntityDefs.add(entityDefStore.preDeleteByGuid(entityDef.getGuid()));
                } else {
                    preDeleteEntityDefs.add(entityDefStore.preDeleteByName(entityDef.getName()));
                }
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getStructDefs())) {
            int i = 0;
            for (AtlasStructDef structDef : typesDef.getStructDefs()) {
                if (StringUtils.isNotBlank(structDef.getGuid())) {
                    structDefStore.deleteByGuid(structDef.getGuid(), preDeleteStructDefs.get(i));
                } else {
                    structDefStore.deleteByName(structDef.getName(), preDeleteStructDefs.get(i));
                }
                i++;
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())) {
            int i = 0;
            for (AtlasClassificationDef classifiDef : typesDef.getClassificationDefs()) {
                if (StringUtils.isNotBlank(classifiDef.getGuid())) {
                    classifiDefStore.deleteByGuid(classifiDef.getGuid(), preDeleteClassifiDefs.get(i));
                } else {
                    classifiDefStore.deleteByName(classifiDef.getName(), preDeleteClassifiDefs.get(i));
                }
                i++;
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEntityDefs())) {
            int i = 0;
            for (AtlasEntityDef entityDef : typesDef.getEntityDefs()) {
                if (StringUtils.isNotBlank(entityDef.getGuid())) {
                    entityDefStore.deleteByGuid(entityDef.getGuid(), preDeleteEntityDefs.get(i));
                } else {
                    entityDefStore.deleteByName(entityDef.getName(), preDeleteEntityDefs.get(i));
                }
                i++;
            }
        }

        if (CollectionUtils.isNotEmpty(typesDef.getEnumDefs())) {
            for (AtlasEnumDef enumDef : typesDef.getEnumDefs()) {
                if (StringUtils.isNotBlank(enumDef.getGuid())) {
                    enumDefStore.deleteByGuid(enumDef.getGuid());
                } else {
                    enumDefStore.deleteByName(enumDef.getName());
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeDefGraphStore.deleteTypesDef(enums={}, structs={}, classfications={}, entities={})",
                    CollectionUtils.size(typesDef.getEnumDefs()),
                    CollectionUtils.size(typesDef.getStructDefs()),
                    CollectionUtils.size(typesDef.getClassificationDefs()),
                    CollectionUtils.size(typesDef.getEntityDefs()));
        }
    }

    @Override
    @GraphTransaction
    public AtlasTypesDef searchTypesDef(SearchFilter searchFilter) throws AtlasBaseException {
        final AtlasTypesDef typesDef = new AtlasTypesDef();
        Predicate searchPredicates = FilterUtil.getPredicateFromSearchFilter(searchFilter);
        try {
            List<AtlasEnumDef> enumDefs = getEnumDefStore(typeRegistry).getAll();
            CollectionUtils.filter(enumDefs, searchPredicates);
            typesDef.setEnumDefs(enumDefs);
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EnumDefs", ex);
        }

        try {
            List<AtlasStructDef> structDefs = getStructDefStore(typeRegistry).getAll();
            Collection typeCollection = CollectionUtils.collect(structDefs, new Transformer() {
                @Override
                public Object transform(Object o) {
                    try {
                        return new AtlasStructType((AtlasStructDef) o, typeRegistry);
                    } catch (AtlasBaseException e) {
                        LOG.warn("Type validation failed for {}", ((AtlasStructDef) o).getName(), e);
                        return null;
                    }
                }
            });
            CollectionUtils.filter(typeCollection, searchPredicates);
            for (Object o : typeCollection) {
                if (o != null)
                    typesDef.getStructDefs().add(((AtlasStructType)o).getStructDef());
            }
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the StructDefs", ex);
        }

        try {
            List<AtlasClassificationDef> classificationDefs = getClassificationDefStore(typeRegistry).getAll();

            Collection typeCollection = CollectionUtils.collect(classificationDefs, new Transformer() {
                @Override
                public Object transform(Object o) {
                    try {
                        return new AtlasClassificationType((AtlasClassificationDef) o, typeRegistry);
                    } catch (AtlasBaseException e) {
                        LOG.warn("Type validation failed for {}", ((AtlasClassificationDef) o).getName(), e);
                        return null;
                    }
                }
            });
            CollectionUtils.filter(typeCollection, searchPredicates);
            for (Object o : typeCollection) {
                if (o != null)
                    typesDef.getClassificationDefs().add(((AtlasClassificationType)o).getClassificationDef());
            }
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the ClassificationDefs", ex);
        }

        try {
            List<AtlasEntityDef> entityDefs = getEntityDefStore(typeRegistry).getAll();
            Collection typeCollection = CollectionUtils.collect(entityDefs, new Transformer() {
                @Override
                public Object transform(Object o) {
                    try {
                        return new AtlasEntityType((AtlasEntityDef) o, typeRegistry);
                    } catch (AtlasBaseException e) {
                        LOG.warn("Type validation failed for {}", ((AtlasEntityDef) o).getName(), e);
                        return null;
                    }
                }
            });
            CollectionUtils.filter(typeCollection, searchPredicates);
            for (Object o : typeCollection) {
                if (o != null)
                    typesDef.getEntityDefs().add(((AtlasEntityType)o).getEntityDef());
            }
        } catch (AtlasBaseException ex) {
            LOG.error("Failed to retrieve the EntityDefs", ex);
        }

        return typesDef;
    }

    @Override
    public void instanceIsActive() throws AtlasException {
        try {
            init();
        } catch (AtlasBaseException e) {
            LOG.error("Failed to init after becoming active", e);
        }
    }

    @Override
    public void instanceIsPassive() throws AtlasException {
        LOG.info("Not reacting to a Passive state change");
    }

    private void bootstrapTypes() {
        AtlasTypeDefStoreInitializer storeInitializer = new AtlasTypeDefStoreInitializer();

        String atlasHomeDir = System.getProperty("atlas.home");
        String typesDirName = (StringUtils.isEmpty(atlasHomeDir) ? "." : atlasHomeDir) + File.separator + "models";

        storeInitializer.initializeStore(this, typeRegistry, typesDirName);
    }

    private AtlasTransientTypeRegistry lockTypeRegistryAndReleasePostCommit() throws AtlasBaseException {
        AtlasTransientTypeRegistry ttr = typeRegistry.lockTypeRegistryForUpdate(typeUpdateLockMaxWaitTimeSeconds);

        new TypeRegistryUpdateHook(ttr);

        return ttr;
    }

    private class TypeRegistryUpdateHook extends GraphTransactionInterceptor.PostTransactionHook {
        private final AtlasTransientTypeRegistry ttr;

        private TypeRegistryUpdateHook(AtlasTransientTypeRegistry ttr) {
            super();

            this.ttr = ttr;
        }

        @Override
        public void onComplete(boolean isSuccess) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> TypeRegistryUpdateHook.onComplete({})", isSuccess);
            }

            typeRegistry.releaseTypeRegistryForUpdate(ttr, isSuccess);

            if (isSuccess) {
                notifyListeners(ttr);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== TypeRegistryUpdateHook.onComplete({})", isSuccess);
            }
        }

        private void notifyListeners(AtlasTransientTypeRegistry ttr) {
            if (CollectionUtils.isNotEmpty(typeDefChangeListeners)) {
                ChangedTypeDefs changedTypeDefs = new ChangedTypeDefs(ttr.getAddedTypes(),
                                                                      ttr.getUpdatedTypes(),
                                                                      ttr.getDeleteedTypes());

                for (TypeDefChangeListener changeListener : typeDefChangeListeners) {
                    try {
                        changeListener.onChange(changedTypeDefs);
                    } catch (Throwable t) {
                        LOG.error("OnChange failed for listener {}", changeListener.getClass().getName(), t);
                    }
                }
            }
        }
    }
}
