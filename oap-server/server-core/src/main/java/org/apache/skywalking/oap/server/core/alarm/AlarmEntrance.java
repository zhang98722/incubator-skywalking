/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.alarm;

import java.util.concurrent.locks.ReentrantLock;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.analysis.indicator.*;
import org.apache.skywalking.oap.server.core.cache.*;
import org.apache.skywalking.oap.server.core.register.*;
import org.apache.skywalking.oap.server.library.module.ModuleManager;

import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.*;

/**
 * @author wusheng
 */
public class AlarmEntrance {
    private ModuleManager moduleManager;
    private ServiceInventoryCache serviceInventoryCache;
    private ServiceInstanceInventoryCache serviceInstanceInventoryCache;
    private EndpointInventoryCache endpointInventoryCache;
    private IndicatorNotify indicatorNotify;
    private ReentrantLock initLock;

    public AlarmEntrance(ModuleManager moduleManager) {
        this.moduleManager = moduleManager;
        this.initLock = new ReentrantLock();
    }

    public void forward(Indicator indicator) {
        if (!moduleManager.has(AlarmModule.NAME)) {
            return;
        }

        init();

        IndicatorMetaInfo indicatorMetaInfo = ((WithMetadata)indicator).getMeta();

        MetaInAlarm metaInAlarm;
        switch (indicatorMetaInfo.getScope()) {
            case SERVICE:
                int serviceId = Integer.parseInt(indicatorMetaInfo.getId());
                ServiceInventory serviceInventory = serviceInventoryCache.get(serviceId);
                ServiceMetaInAlarm serviceMetaInAlarm = new ServiceMetaInAlarm();
                serviceMetaInAlarm.setIndicatorName(indicatorMetaInfo.getIndicatorName());
                serviceMetaInAlarm.setId(serviceId);
                serviceMetaInAlarm.setName(serviceInventory.getName());
                metaInAlarm = serviceMetaInAlarm;
                break;
            case SERVICE_INSTANCE:
                int serviceInstanceId = Integer.parseInt(indicatorMetaInfo.getId());
                ServiceInstanceInventory serviceInstanceInventory = serviceInstanceInventoryCache.get(serviceInstanceId);
                ServiceInstanceMetaInAlarm instanceMetaInAlarm = new ServiceInstanceMetaInAlarm();
                instanceMetaInAlarm.setIndicatorName(indicatorMetaInfo.getIndicatorName());
                instanceMetaInAlarm.setId(serviceInstanceId);
                instanceMetaInAlarm.setName(serviceInstanceInventory.getName());
                metaInAlarm = instanceMetaInAlarm;
                break;
            case ENDPOINT:
                int endpointId = Integer.parseInt(indicatorMetaInfo.getId());
                EndpointInventory endpointInventory = endpointInventoryCache.get(endpointId);
                EndpointMetaInAlarm endpointMetaInAlarm = new EndpointMetaInAlarm();
                endpointMetaInAlarm.setIndicatorName(indicatorMetaInfo.getIndicatorName());
                endpointMetaInAlarm.setId(endpointId);

                serviceId = endpointInventory.getServiceId();
                serviceInventory = serviceInventoryCache.get(serviceId);

                String textName = endpointInventory.getName() + " in " + serviceInventory.getName();

                endpointMetaInAlarm.setName(textName);
                metaInAlarm = endpointMetaInAlarm;
                break;
            default:
                return;
        }

        indicatorNotify.notify(metaInAlarm, indicator);
    }

    private void init() {
        if (serviceInventoryCache == null) {
            initLock.lock();
            try {
                if (serviceInventoryCache == null) {
                    serviceInventoryCache = moduleManager.find(CoreModule.NAME).provider().getService(ServiceInventoryCache.class);
                    serviceInstanceInventoryCache = moduleManager.find(CoreModule.NAME).provider().getService(ServiceInstanceInventoryCache.class);
                    endpointInventoryCache = moduleManager.find(CoreModule.NAME).provider().getService(EndpointInventoryCache.class);
                    indicatorNotify = moduleManager.find(AlarmModule.NAME).provider().getService(IndicatorNotify.class);
                }
            } finally {
                initLock.unlock();
            }
        }
    }
}
