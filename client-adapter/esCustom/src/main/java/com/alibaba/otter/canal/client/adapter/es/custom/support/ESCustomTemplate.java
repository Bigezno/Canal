package com.alibaba.otter.canal.client.adapter.es.custom.support;

import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ESCustomTemplate  {

    private static final int                                  MAX_BATCH_SIZE = 1000;

    private ESConnection esConnection;

    private ESBulkRequest esBulkRequest;

    // es 字段类型本地缓存
    private static ConcurrentMap<String, Map<String, String>> esFieldTypes   = new ConcurrentHashMap<>();

    public ESCustomTemplate(ESConnection esConnection){
        this.esConnection = esConnection;
        this.esBulkRequest = this.esConnection.new ES8xBulkRequest();
    }

    public ESBulkRequest getBulk() {
        return esBulkRequest;
    }

    public void resetBulkRequestBuilder() {
        this.esBulkRequest.resetBulk();
    }

    /**
     * 如果大于批量数则提交批次
     */
    private void commitBulk() {
        if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
            commit();
        }
    }

    public void commit() {
        if (getBulk().numberOfActions() > 0) {
            ESBulkRequest.ESBulkResponse response = getBulk().bulk();
            if (response.hasFailures()) {
                response.processFailBulkResponse("ES sync commit error ");
            }
            resetBulkRequestBuilder();
        }
    }

    /**
     *  根据索引中字段类型
     * @param index
     * @param fieldName
     * @return
     */
    public String getEsType(String index, String fieldName) {
        String key = index + "-" + "_doc";
        Map<String, String> fieldType = esFieldTypes.get(key);
        if (fieldType != null) {
            return fieldType.get(fieldName);
        } else {
            MappingMetadata mappingMetaData = esConnection.getMapping(index);
            if (mappingMetaData == null) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + index);
            }
            fieldType = new LinkedHashMap<>();
            Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
            Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
            for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                if (value.containsKey("properties")) {
                    fieldType.put(entry.getKey(), "object");
                } else {
                    fieldType.put(entry.getKey(), (String) value.get("type"));
                }
            }
            esFieldTypes.put(key, fieldType);
            return fieldType.get(fieldName);
        }
    }


    /**
     *  新增数据
     * @param pkVal
     * @param esFieldData
     */
    public void insert(String esIndex, Object pkVal, Map<String, Object> esFieldData) {
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));

        String parentVal = (String) esFieldData.remove("$parent_routing");
        ESBulkRequest.ESIndexRequest indexRequest = esConnection.new ES8xIndexRequest(esIndex, pkVal.toString()).setSource(esFieldDataTmp);
        if (StringUtils.isNotEmpty(parentVal)) {
            indexRequest.setRouting(parentVal);
        }
        getBulk().add(indexRequest);

        commitBulk();
    }


    /**
     * 更新数据
     * @param pkVal
     * @param esFieldData
     */
    public void update(String esIndex, Object pkVal, Map<String, Object> esFieldData) {
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));

        String parentVal = (String) esFieldData.remove("$parent_routing");
        ESBulkRequest.ESUpdateRequest esUpdateRequest = this.esConnection.new ES8xUpdateRequest(esIndex, pkVal.toString()).setDoc(esFieldDataTmp);
        if (StringUtils.isNotEmpty(parentVal)) {
            esUpdateRequest.setRouting(parentVal);
        }
        getBulk().add(esUpdateRequest);

        commitBulk();
    }


    /**
     *  删除数据
     * @param pkVal
     */
    public void delete(String esIndex, Object pkVal) {
        ESBulkRequest.ESDeleteRequest esDeleteRequest = this.esConnection.new ES8xDeleteRequest(esIndex, pkVal.toString());
        getBulk().add(esDeleteRequest);
        commitBulk();
    }


}
