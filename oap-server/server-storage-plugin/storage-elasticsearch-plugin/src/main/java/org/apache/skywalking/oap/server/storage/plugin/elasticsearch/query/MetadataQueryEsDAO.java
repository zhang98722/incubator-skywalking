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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.query;

import com.google.common.base.Strings;
import com.google.gson.*;

import java.io.IOException;
import java.util.*;

import org.apache.skywalking.oap.server.core.query.entity.*;
import org.apache.skywalking.oap.server.core.register.*;
import org.apache.skywalking.oap.server.core.source.DetectPoint;
import org.apache.skywalking.oap.server.core.storage.query.IMetadataQueryDAO;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.library.util.BooleanUtils;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.*;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.apache.skywalking.oap.server.core.register.ServiceInstanceInventory.PropertyUtil.*;

/**
 * @author peng-yongsheng
 */
public class MetadataQueryEsDAO extends EsDAO implements IMetadataQueryDAO {
    private static final Gson GSON = new Gson();

    public MetadataQueryEsDAO(ElasticSearchClient client) {
        super(client);
    }

    @Override public int numOfService(long startTimestamp, long endTimestamp) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(timeRangeQueryBuild(startTimestamp, endTimestamp));

        boolQueryBuilder.must().add(QueryBuilders.termQuery(ServiceInventory.IS_ADDRESS, BooleanUtils.FALSE));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(0);

        SearchResponse response = getClient().search(ServiceInventory.MODEL_NAME, sourceBuilder);
        return (int)response.getHits().getTotalHits();
    }

    @Override public int numOfEndpoint(long startTimestamp, long endTimestamp) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        boolQueryBuilder.must().add(QueryBuilders.termQuery(EndpointInventory.DETECT_POINT, DetectPoint.SERVER.ordinal()));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(0);

        SearchResponse response = getClient().search(EndpointInventory.MODEL_NAME, sourceBuilder);
        return (int)response.getHits().getTotalHits();
    }

    @Override
    public int numOfConjectural(long startTimestamp, long endTimestamp, int nodeTypeValue) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        sourceBuilder.query(QueryBuilders.termQuery(ServiceInventory.NODE_TYPE, nodeTypeValue));
        sourceBuilder.size(0);

        SearchResponse response = getClient().search(ServiceInventory.MODEL_NAME, sourceBuilder);

        return (int)response.getHits().getTotalHits();
    }

    @Override
    public List<Service> getAllServices(long startTimestamp, long endTimestamp) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(timeRangeQueryBuild(startTimestamp, endTimestamp));

        boolQueryBuilder.must().add(QueryBuilders.termQuery(ServiceInventory.IS_ADDRESS, BooleanUtils.FALSE));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(100);

        SearchResponse response = getClient().search(ServiceInventory.MODEL_NAME, sourceBuilder);

        return buildServices(response);
    }

    @Override
    public List<Database> getAllDatabases() throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(QueryBuilders.termQuery(ServiceInventory.NODE_TYPE, NodeType.Database.value()));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(100);

        SearchResponse response = getClient().search(ServiceInventory.MODEL_NAME, sourceBuilder);

        List<Database> databases = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Database database = new Database();
            database.setId(((Number) sourceAsMap.get(ServiceInventory.SEQUENCE)).intValue());
            database.setName((String) sourceAsMap.get(ServiceInventory.NAME));
            String propertiesString = (String) sourceAsMap.get(ServiceInstanceInventory.PROPERTIES);
            if (!Strings.isNullOrEmpty(propertiesString)) {
                JsonObject properties = GSON.fromJson(propertiesString, JsonObject.class);
                if (properties.has(ServiceInventory.PropertyUtil.DATABASE)) {
                    database.setType(properties.get(ServiceInventory.PropertyUtil.DATABASE).getAsString());
                } else {
                    database.setType("UNKNOWN");
                }
            }
            databases.add(database);
        }
        return databases;
    }

    @Override public List<Service> searchServices(long startTimestamp, long endTimestamp,
        String keyword) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(timeRangeQueryBuild(startTimestamp, endTimestamp));
        boolQueryBuilder.must().add(QueryBuilders.termQuery(ServiceInventory.IS_ADDRESS, BooleanUtils.FALSE));

        if (!Strings.isNullOrEmpty(keyword)) {
            String matchCName = MatchCNameBuilder.INSTANCE.build(ServiceInventory.NAME);
            boolQueryBuilder.must().add(QueryBuilders.matchQuery(matchCName, keyword));
        }

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(100);

        SearchResponse response = getClient().search(ServiceInventory.MODEL_NAME, sourceBuilder);
        return buildServices(response);
    }

    @Override
    public Service searchService(String serviceCode) throws IOException {
        GetResponse response = getClient().get(ServiceInventory.MODEL_NAME, ServiceInventory.buildId(serviceCode));
        if (response.isExists()) {
            Service service = new Service();
            service.setId(((Number)response.getSource().get(ServiceInventory.SEQUENCE)).intValue());
            service.setName((String)response.getSource().get(ServiceInventory.NAME));
            return service;
        } else {
            return null;
        }
    }

    @Override public List<Endpoint> searchEndpoint(String keyword, String serviceId,
        int limit) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(QueryBuilders.termQuery(EndpointInventory.SERVICE_ID, serviceId));

        if (!Strings.isNullOrEmpty(keyword)) {
            String matchCName = MatchCNameBuilder.INSTANCE.build(EndpointInventory.NAME);
            boolQueryBuilder.must().add(QueryBuilders.matchQuery(matchCName, keyword));
        }

        boolQueryBuilder.must().add(QueryBuilders.termQuery(EndpointInventory.DETECT_POINT, DetectPoint.SERVER.ordinal()));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(limit);

        SearchResponse response = getClient().search(EndpointInventory.MODEL_NAME, sourceBuilder);

        List<Endpoint> endpoints = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            Endpoint endpoint = new Endpoint();
            endpoint.setId(((Number)sourceAsMap.get(EndpointInventory.SEQUENCE)).intValue());
            endpoint.setName((String)sourceAsMap.get(EndpointInventory.NAME));
            endpoints.add(endpoint);
        }

        return endpoints;
    }

    @Override public List<ServiceInstance> getServiceInstances(long startTimestamp, long endTimestamp,
        String serviceId) throws IOException {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource();

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must().add(timeRangeQueryBuild(startTimestamp, endTimestamp));

        boolQueryBuilder.must().add(QueryBuilders.termQuery(ServiceInstanceInventory.SERVICE_ID, serviceId));

        sourceBuilder.query(boolQueryBuilder);
        sourceBuilder.size(100);

        SearchResponse response = getClient().search(ServiceInstanceInventory.MODEL_NAME, sourceBuilder);

        List<ServiceInstance> serviceInstances = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            ServiceInstance serviceInstance = new ServiceInstance();
            serviceInstance.setId(String.valueOf(sourceAsMap.get(ServiceInstanceInventory.SEQUENCE)));
            serviceInstance.setName((String)sourceAsMap.get(ServiceInstanceInventory.NAME));

            String propertiesString = (String)sourceAsMap.get(ServiceInstanceInventory.PROPERTIES);
            if (!Strings.isNullOrEmpty(propertiesString)) {
                JsonObject properties = GSON.fromJson(propertiesString, JsonObject.class);
                if (properties.has(LANGUAGE)) {
                    serviceInstance.setLanguage(LanguageTrans.INSTANCE.value(properties.get(LANGUAGE).getAsString()));
                } else {
                    serviceInstance.setLanguage(Language.UNKNOWN);
                }

                if (properties.has(OS_NAME)) {
                    serviceInstance.getAttributes().add(new Attribute(OS_NAME, properties.get(OS_NAME).getAsString()));
                }
                if (properties.has(HOST_NAME)) {
                    serviceInstance.getAttributes().add(new Attribute(HOST_NAME, properties.get(HOST_NAME).getAsString()));
                }
                if (properties.has(PROCESS_NO)) {
                    serviceInstance.getAttributes().add(new Attribute(PROCESS_NO, properties.get(PROCESS_NO).getAsString()));
                }
                if (properties.has(IPV4S)) {
                    List<String> ipv4s = ServiceInstanceInventory.PropertyUtil.ipv4sDeserialize(properties.get(IPV4S).getAsString());
                    for (String ipv4 : ipv4s) {
                        serviceInstance.getAttributes().add(new Attribute(ServiceInstanceInventory.PropertyUtil.IPV4S, ipv4));
                    }
                }
            } else {
                serviceInstance.setLanguage(Language.UNKNOWN);
            }

            serviceInstances.add(serviceInstance);
        }

        return serviceInstances;
    }

    private List<Service> buildServices(SearchResponse response) {
        List<Service> services = new ArrayList<>();
        for (SearchHit searchHit : response.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            Service service = new Service();
            service.setId(((Number)sourceAsMap.get(ServiceInventory.SEQUENCE)).intValue());
            service.setName((String)sourceAsMap.get(ServiceInventory.NAME));
            services.add(service);
        }

        return services;
    }

    private BoolQueryBuilder timeRangeQueryBuild(long startTimestamp, long endTimestamp) {
        BoolQueryBuilder boolQuery1 = QueryBuilders.boolQuery();
        boolQuery1.must().add(QueryBuilders.rangeQuery(RegisterSource.HEARTBEAT_TIME).gte(endTimestamp));
        boolQuery1.must().add(QueryBuilders.rangeQuery(RegisterSource.REGISTER_TIME).lte(endTimestamp));

        BoolQueryBuilder boolQuery2 = QueryBuilders.boolQuery();
        boolQuery2.must().add(QueryBuilders.rangeQuery(RegisterSource.REGISTER_TIME).lte(endTimestamp));
        boolQuery2.must().add(QueryBuilders.rangeQuery(RegisterSource.HEARTBEAT_TIME).gte(startTimestamp));

        BoolQueryBuilder timeBoolQuery = QueryBuilders.boolQuery();
        timeBoolQuery.should().add(boolQuery1);
        timeBoolQuery.should().add(boolQuery2);

        return timeBoolQuery;
    }
}
