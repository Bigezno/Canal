package com.alibaba.otter.canal.client.adapter.es.custom.index;

import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.es.custom.support.ESCustomTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class AbstractIndexService implements IndexService {

    protected ESCustomTemplate esCustomTemplate;

    public AbstractIndexService(ESCustomTemplate esCustomTemplate) {
        this.esCustomTemplate = esCustomTemplate;
    }

    public Object getValFromDatas(String esIndex, EsIndexConfig.EsMapping esMapping, List<Map<String, Object>> dmlDatas, String fieldName) {
        if (dmlDatas == null || dmlDatas.isEmpty()) {
            return null;
        }
        for (Map<String, Object> dmlData : dmlDatas) {
            if (dmlData.containsKey(fieldName)) {
                return getValFromData(esIndex, esMapping.getObjFields(), dmlData, fieldName);
            }
        }
        return null;
    }


    public Object getValFromData(String esIndex, Map<String, String> objFields, Map<String, Object> dmlData, String fieldName) {
        String esType = esCustomTemplate.getEsType(esIndex, fieldName);
        Object value = dmlData.get(fieldName);
        if (value instanceof Byte) {
            if ("boolean".equals(esType)) {
                value = ((Byte) value).intValue() != 0;
            }
        }

        // 如果是对象类型
        if (objFields.containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, objFields.get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }


    public Object getValFromRS(String esIndex, Map<String, String> objFields, ResultSet resultSet, String fieldName) throws SQLException {
        fieldName = Util.cleanColumn(fieldName);
        String esType = esCustomTemplate.getEsType(esIndex, fieldName);

        Object value = resultSet.getObject(fieldName);
        if (value instanceof Boolean) {
            if (!"boolean".equals(esType)) {
                value = resultSet.getByte(fieldName);
            }
        }

        // 如果是对象类型
        if (objFields != null && objFields.containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, objFields.get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }



    protected boolean needUpdateEsIndex(List<Map<String, Object>> oldList, EsIndexConfig.TableEsMapping tableEsMapping) {
        EsIndexConfig.EsMapping esMapping = tableEsMapping.getEsMapping();

        List<String> esFields = Arrays.stream(esMapping.getEsFields().split(",")).collect(Collectors.toList());

        if (!esMapping.getObjFields().isEmpty()) {
            esFields.removeAll(esMapping.getObjFields().keySet());
        }

        if (StringUtils.isNotEmpty(esMapping.getWhiteListFields())) {
            List<String> whiteListFields = Arrays.stream(esMapping.getWhiteListFields().split(",")).collect(Collectors.toList());
            esFields.addAll(whiteListFields);
        }

        if (!tableEsMapping.getRelationFieldList().isEmpty()) {
            esFields.addAll(tableEsMapping.getRelationFieldList());
        }

        for (String fieldName : esFields) {
            for (Map<String, Object> oldData : oldList) {
                if (oldData == null || oldData.isEmpty()) {
                    continue;
                }
                if (oldData.containsKey(fieldName)) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * 获取修改的关联字段
     * @param oldList
     * @param tableEsMapping
     * @return
     */
    protected List<String> getUpdatedRelationFieldList(List<Map<String, Object>> oldList, EsIndexConfig.TableEsMapping tableEsMapping) {
        List<String> updatedRelationFields = new ArrayList<>();
        if (oldList == null || oldList.isEmpty()) {
            return updatedRelationFields;
        }

        List<String> relationFields = tableEsMapping.getRelationFieldList();
        for (Map<String, Object> oldData : oldList) {
            if (oldData == null || oldData.isEmpty()) {
                continue;
            }
            for (String updatedField : oldData.keySet()) {
                if (relationFields.contains(updatedField)) {
                    updatedRelationFields.add(updatedField);
                }
            }
        }

        return updatedRelationFields;
    }


    /**
     *  获取需要更新的es字段集合
     * @param exIndex
     * @param esMapping
     * @param dml
     * @return
     */
    protected Map<String, Object> getEsFieldData(String exIndex, EsIndexConfig.EsMapping esMapping, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();

        Map<String, Object> esFieldData = new HashMap<>();
        List<String> esFields = Arrays.stream(esMapping.getEsFields().split(",")).collect(Collectors.toList());
        for (String esField : esFields) {
            for (Map<String, Object> oldData : oldList) {
                if (oldData.containsKey(esField)) {
                    esFieldData.put(esField, getValFromDatas(exIndex, esMapping, dataList, esField));
                }
            }
        }
        return esFieldData;
    }


    public Object getMainTableEsIdVal(String esIndex, EsIndexConfig.TableEsMapping tableEsMapping, List<Map<String, Object>> dataList) {
        if (tableEsMapping.isMainTable()) {
            return getValFromDatas(esIndex, tableEsMapping.getEsMapping(), dataList, tableEsMapping.getEsId());
        }
        return null;
    }


}
