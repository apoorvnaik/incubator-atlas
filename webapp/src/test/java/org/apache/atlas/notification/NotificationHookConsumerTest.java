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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.web.service.ServiceState;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class NotificationHookConsumerTest {

    @Mock
    private NotificationInterface notificationInterface;

    @Mock
    private Configuration configuration;

    @Mock
    private ExecutorService executorService;

    @Mock
    private AtlasEntityStore atlasEntityStore;

    @Mock
    private ServiceState serviceState;

    @Mock
    private AtlasFormatConverters instanceFormatters;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @BeforeMethod
    public void setup() throws AtlasBaseException {
        MockitoAnnotations.initMocks(this);
        AtlasFormatConverter converter = mock(AtlasFormatConverter.class);
        AtlasType mockType = mock(AtlasType.class);
        when(typeRegistry.getType(anyString())).thenReturn(mockType);
        when(instanceFormatters.getConverter(TypeCategory.ENTITY)).thenReturn(converter);
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(converter.fromV1ToV2(any(Referenceable.class), any(AtlasType.class))).thenReturn(mockEntity);
    }

    @Test
    public void testConsumerCanProceedIfServerIsReady() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        assertTrue(hookConsumer.serverAvailable(timer));

        verifyZeroInteractions(timer);
    }

    @Test
    public void testConsumerWaitsNTimesIfServerIsNotReadyNTimes() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);

        when(serviceState.getState())
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.PASSIVE)
                .thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        assertTrue(hookConsumer.serverAvailable(timer));

        verify(timer, times(3)).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
    }

    @Test
    public void testCommitIsCalledWhenMessageIsProcessed() throws AtlasServiceException, AtlasException {
        NotificationHookConsumer notificationHookConsumer =
                new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        NotificationConsumer consumer = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(consumer);
        HookNotification.EntityCreateRequest message = mock(HookNotification.EntityCreateRequest.class);
        when(message.getUser()).thenReturn("user");
        when(message.getType()).thenReturn(HookNotification.HookNotificationType.ENTITY_CREATE);

        hookConsumer.handleMessage(message);

        verify(consumer).commit();
    }

    @Test
    public void testCommitIsNotCalledEvenWhenMessageProcessingFails() throws AtlasServiceException, AtlasException, AtlasBaseException {
        NotificationHookConsumer notificationHookConsumer =
                new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        NotificationConsumer consumer = mock(NotificationConsumer.class);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(consumer);
        HookNotification.EntityCreateRequest message = new HookNotification.EntityCreateRequest("user", new ArrayList<Referenceable>());
        when(atlasEntityStore.createOrUpdate(anyList())).thenThrow(new RuntimeException("Simulating exception in processing message"));
        hookConsumer.handleMessage(message);

        verifyZeroInteractions(consumer);
    }

    @Test
    public void testConsumerProceedsWithFalseIfInterrupted() throws Exception {
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        NotificationHookConsumer.HookConsumer hookConsumer =
                notificationHookConsumer.new HookConsumer(mock(NotificationConsumer.class));
        NotificationHookConsumer.Timer timer = mock(NotificationHookConsumer.Timer.class);
        doThrow(new InterruptedException()).when(timer).sleep(NotificationHookConsumer.SERVER_READY_WAIT_TIME_MS);
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        assertFalse(hookConsumer.serverAvailable(timer));
    }

    @Test
    public void testConsumersStartedIfHAIsDisabled() throws Exception {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        notificationHookConsumer.startInternal(configuration, executorService);
        verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK, 1);
        verify(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreNotStartedIfHAIsEnabled() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        notificationHookConsumer.startInternal(configuration, executorService);
        verifyZeroInteractions(notificationInterface);
    }

    @Test
    public void testConsumersAreStartedWhenInstanceBecomesActive() throws Exception {
        when(configuration.containsKey(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsActive();
        verify(notificationInterface).createConsumers(NotificationInterface.NotificationType.HOOK, 1);
        verify(executorService).submit(any(NotificationHookConsumer.HookConsumer.class));
    }

    @Test
    public void testConsumersAreStoppedWhenInstanceBecomesPassive() throws Exception {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(configuration.getInt(NotificationHookConsumer.CONSUMER_THREADS_PROPERTY, 1)).thenReturn(1);
        List<NotificationConsumer<Object>> consumers = new ArrayList();
        consumers.add(mock(NotificationConsumer.class));
        when(notificationInterface.createConsumers(NotificationInterface.NotificationType.HOOK, 1)).
                thenReturn(consumers);
        NotificationHookConsumer notificationHookConsumer = new NotificationHookConsumer(notificationInterface, atlasEntityStore, serviceState, instanceFormatters, typeRegistry);
        notificationHookConsumer.startInternal(configuration, executorService);
        notificationHookConsumer.instanceIsPassive();
        verify(notificationInterface).close();
        verify(executorService).shutdown();
    }
}
