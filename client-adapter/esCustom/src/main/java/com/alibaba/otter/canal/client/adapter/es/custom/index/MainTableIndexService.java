package com.alibaba.otter.canal.client.adapter.es.custom.index;

import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.es.custom.support.ESCustomTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MainTableIndexService extends AbstractIndexService {

    private static Logger logger = LoggerFactory.getLogger(MainTableIndexService.class);

    public MainTableIndexService(ESCustomTemplate esCustomTemplate) {
        super(esCustomTemplate);
    }

    public void insert(String esIndex, EsIndexConfig.TableEsMapping mainTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        if (!Objects.equals(mainTable.getTableName(), dml.getTable())) {
            logger.error("insert method found mainTable:{}, but not match the dml table:{}", mainTable.getTableName(), dml.getTable());
            return;
        }

        Object resultIdVal = getMainTableEsIdVal(esIndex, mainTable, dataList);
        if (resultIdVal == null) {
            logger.error("insert error, not found esId in dml data, please check your config");
            return;
        }

        Map<String, Object> esFieldData = new HashMap<>();
        String[] fieldNames = mainTable.getEsMapping().getEsFields().split(",");
        for (String fieldName : fieldNames) {
            Object fieldValue = getValFromDatas(esIndex, mainTable.getEsMapping(), dml.getData(), fieldName);
            esFieldData.put(fieldName, fieldValue);
        }

        logger.info("insert data to index:{}, table: {}, id: {}, insertFields:{}",
                esIndex, dml.getTable(), resultIdVal, esFieldData);

        esCustomTemplate.insert(esIndex, resultIdVal, esFieldData);

        // 关联字段的更新
        relationInsert(esIndex, mainTable, resultIdVal, dataList);
    }


    public void update(String esIndex, EsIndexConfig.TableEsMapping mainTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }

        if (!Objects.equals(mainTable.getTableName(), dml.getTable())) {
            logger.error("update method found mainTable:{}, but not match the dml table:{}", mainTable.getTableName(), dml.getTable());
            return;
        }

        // 如果修改的字段不是esFields和白名单中的字段，则不需要更新索引
        if (!needUpdateEsIndex(oldList, mainTable)) {
            return;
        }

        Object resultIdVal = getMainTableEsIdVal(esIndex, mainTable, dataList);
        if (resultIdVal == null) {
            logger.error("update error, not found esId in dml data, please check your config");
            return;
        }

        Map<String, Object> esFieldData = getEsFieldData(esIndex, mainTable.getEsMapping(), dml);

        Object oldEsIdVal = getMainTableEsIdVal(esIndex, mainTable, oldList);
        // 主表esId的值更新了，删除旧的文档，索引新的文档
        if (oldEsIdVal != null && !Objects.equals(resultIdVal, oldEsIdVal)) {
            logger.info("newEsId:{}, oldEsId:{}", resultIdVal, oldEsIdVal);
            insert(esIndex, mainTable, dml);
            esCustomTemplate.delete(esIndex, oldEsIdVal);
            return;
        }

        if (!esFieldData.isEmpty()) {
            logger.info(" update data from index:{}, table: {}, id: {}, updateField:{}",
                    esIndex, dml.getTable(), resultIdVal, esFieldData);
            esCustomTemplate.update(esIndex, resultIdVal, esFieldData);
        }

        // 关联字段更新
        relationUpdate(esIndex, mainTable, dml);
    }


    public void relationInsert(String esIndex, EsIndexConfig.TableEsMapping mainTable, Object resultIdVal, List<Map<String, Object>> dataList) {
        EsIndexConfig.EsMapping esMapping = mainTable.getEsMapping();

        for (EsIndexConfig.Relation relation : mainTable.getRelations()) {
            DataSource relationDS = DatasourceConfig.DATA_SOURCES.get(relation.getDataSourceKey());

            // 关联字段
            List<String> relationFields = Arrays.stream(relation.getRelationFields().split(",")).collect(Collectors.toList());
            List<Object> valueList = new ArrayList<>();
            for (String relationField : relationFields) {
                Object fieldValue = getValFromDatas(esIndex, esMapping, dataList, relationField);
                valueList.add(fieldValue);
            }

            Function<ResultSet, Object> function = buildRelationUpdateFunction(esIndex, resultIdVal, relation);
            int syncCount = (Integer) Util.sqlRS(relationDS, relation.getSql(), valueList, function);

            logger.info("sync relation sql:{}, count:{}", relation.getSql(), syncCount);
        }
    }


    public void relationUpdate(String esIndex, EsIndexConfig.TableEsMapping mainTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();

        List<String> updatedRelationFields = getUpdatedRelationFieldList(oldList, mainTable);
        if (CollectionUtils.isEmpty(updatedRelationFields)) {
            return;
        }

        Object resultIdVal = getMainTableEsIdVal(esIndex, mainTable, dataList);

        // 更新了关联字段
        for (String updatedRelationField : updatedRelationFields) {
            for (EsIndexConfig.Relation relation : mainTable.getRelations()) {
                List<String> relationFields = Arrays.stream(relation.getRelationFields().split(",")).collect(Collectors.toList());
                if (!relationFields.contains(updatedRelationField)) {
                    continue;
                }

                DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(relation.getDataSourceKey());

                List<Object> valueList = new ArrayList<>();
                for (String conditionField : relation.getRelationFields().split(",")) {
                    Object value = getValFromDatas(esIndex, mainTable.getEsMapping(), dataList, conditionField);
                    valueList.add(value);
                }

                Function<ResultSet, Object> function = buildRelationUpdateFunction(esIndex, resultIdVal, relation);
                int syncCount = (Integer) Util.sqlRS(dataSource, relation.getSql(), valueList, function);

                logger.info("relate update data from index:{}, table: {}, id: {}, relationId:{}, count:{}",
                        esIndex, dml.getTable(), resultIdVal, updatedRelationField, syncCount);

            }
        }
    }


    public Function<ResultSet, Object> buildRelationUpdateFunction(
            String esIndex, Object resultIdVal, EsIndexConfig.Relation relation) {
        String[] esFields = relation.getEsFields().split(",");
        Map<String, String> objFields = relation.getObjFields();

        return rs -> {
            int count = 0;
            try {
                boolean hasData = false;
                while (rs.next()) {
                    hasData = true;

                    Map<String, Object> relationESFieldData = new HashMap<>();
                    for (String fileName : esFields) {
                        Object searchVal = getValFromRS(esIndex, objFields, rs, fileName);
                        relationESFieldData.put(fileName, searchVal);
                    }

                    logger.info("relate update data from index:{}, id: {}, relationFields:{}",
                            esIndex, resultIdVal, relationESFieldData);

                    esCustomTemplate.update(esIndex, resultIdVal, relationESFieldData);

                    count++;
                }

                if (!hasData) {
                    Map<String, Object> relationESFieldData = new HashMap<>();
                    for (String fileName : esFields) {
                        relationESFieldData.put(fileName, null);
                    }

                    logger.info("relate update data from index:{}, id: {}, relationFields:{}",
                            esIndex, resultIdVal, relationESFieldData);

                    esCustomTemplate.update(esIndex, resultIdVal, relationESFieldData);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            return count;
        };
    }


    public void delete(String esIndex, EsIndexConfig.TableEsMapping mainTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        if (!Objects.equals(mainTable.getTableName(), dml.getTable())) {
            logger.error("delete method found mainTable:{}, but not match the dml table:{}", mainTable.getTableName(), dml.getTable());
            return;
        }

        Object resultIdVal = getMainTableEsIdVal(esIndex, mainTable, dataList);
        if (resultIdVal == null) {
            logger.error("delete error, not found esId in dml data, please check your config");
            return;
        }

        logger.info("delete data from index:{}, table: {}, id: {}", esIndex, dml.getTable(), resultIdVal);
        esCustomTemplate.delete(esIndex, resultIdVal);
    }

}
