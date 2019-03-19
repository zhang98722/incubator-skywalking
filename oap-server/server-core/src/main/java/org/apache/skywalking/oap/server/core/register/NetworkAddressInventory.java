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

package org.apache.skywalking.oap.server.core.register;

import java.util.*;
import lombok.*;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.register.annotation.InventoryType;
import org.apache.skywalking.oap.server.core.remote.annotation.StreamData;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteData;
import org.apache.skywalking.oap.server.core.source.*;
import org.apache.skywalking.oap.server.core.storage.StorageBuilder;
import org.apache.skywalking.oap.server.core.storage.annotation.*;
import org.elasticsearch.common.Strings;

import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.NETWORK_ADDRESS;

/**
 * @author peng-yongsheng
 */
@InventoryType
@StreamData
@ScopeDeclaration(id = NETWORK_ADDRESS, name = "NetworkAddress")
@StorageEntity(name = NetworkAddressInventory.MODEL_NAME, builder = NetworkAddressInventory.Builder.class, deleteHistory = false, sourceScopeId = DefaultScopeDefine.NETWORK_ADDRESS)
public class NetworkAddressInventory extends RegisterSource {

    public static final String MODEL_NAME = "network_address_inventory";

    private static final String NAME = "name";
    public static final String NODE_TYPE = "node_type";

    @Setter @Getter @Column(columnName = NAME, matchQuery = true) private String name = Const.EMPTY_STRING;
    @Setter(AccessLevel.PRIVATE) @Getter(AccessLevel.PRIVATE) @Column(columnName = NODE_TYPE) private int nodeType;

    public void setNetworkAddressNodeType(NodeType nodeType) {
        this.nodeType = nodeType.value();
    }

    public NodeType getNetworkAddressNodeType() {
        return NodeType.get(this.nodeType);
    }

    public static String buildId(String networkAddress) {
        return networkAddress;
    }

    @Override public String id() {
        return buildId(name);
    }

    @Override public int hashCode() {
        int result = 17;
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        NetworkAddressInventory source = (NetworkAddressInventory)obj;
        if (!name.equals(source.getName()))
            return false;

        return true;
    }

    public NetworkAddressInventory getClone() {
        NetworkAddressInventory inventory = new NetworkAddressInventory();
        inventory.setSequence(getSequence());
        inventory.setRegisterTime(getRegisterTime());
        inventory.setHeartbeatTime(getHeartbeatTime());
        inventory.setName(name);
        inventory.setNodeType(nodeType);

        return inventory;
    }

    @Override public boolean combine(RegisterSource registerSource) {
        boolean isCombine = super.combine(registerSource);
        NetworkAddressInventory inventory = (NetworkAddressInventory)registerSource;

        if (nodeType != inventory.nodeType) {
            setNodeType(inventory.nodeType);
            return true;
        } else {
            return isCombine;
        }
    }

    @Override public RemoteData.Builder serialize() {
        RemoteData.Builder remoteBuilder = RemoteData.newBuilder();
        remoteBuilder.addDataIntegers(getSequence());
        remoteBuilder.addDataIntegers(getNodeType());

        remoteBuilder.addDataLongs(getRegisterTime());
        remoteBuilder.addDataLongs(getHeartbeatTime());

        remoteBuilder.addDataStrings(Strings.isNullOrEmpty(name) ? Const.EMPTY_STRING : name);
        return remoteBuilder;
    }

    @Override public void deserialize(RemoteData remoteData) {
        setSequence(remoteData.getDataIntegers(0));
        setNodeType(remoteData.getDataIntegers(1));

        setRegisterTime(remoteData.getDataLongs(0));
        setHeartbeatTime(remoteData.getDataLongs(1));

        setName(remoteData.getDataStrings(0));
    }

    @Override public int remoteHashCode() {
        return 0;
    }

    public static class Builder implements StorageBuilder<NetworkAddressInventory> {

        @Override public NetworkAddressInventory map2Data(Map<String, Object> dbMap) {
            NetworkAddressInventory inventory = new NetworkAddressInventory();
            inventory.setSequence((Integer)dbMap.get(SEQUENCE));
            inventory.setName((String)dbMap.get(NAME));
            inventory.setNodeType((Integer)dbMap.get(NODE_TYPE));
            inventory.setRegisterTime((Long)dbMap.get(REGISTER_TIME));
            inventory.setHeartbeatTime((Long)dbMap.get(HEARTBEAT_TIME));
            return inventory;
        }

        @Override public Map<String, Object> data2Map(NetworkAddressInventory storageData) {
            Map<String, Object> map = new HashMap<>();
            map.put(SEQUENCE, storageData.getSequence());
            map.put(NAME, storageData.getName());
            map.put(NODE_TYPE, storageData.getNodeType());
            map.put(REGISTER_TIME, storageData.getRegisterTime());
            map.put(HEARTBEAT_TIME, storageData.getHeartbeatTime());
            return map;
        }
    }
}
