package com.alibaba.otter.canal.client.adapter.es.custom.index;

import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.es.custom.support.ESCustomTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

public class RelationTableIndexService extends AbstractIndexService {

    private static Logger logger = LoggerFactory.getLogger(RelationTableIndexService.class);

    public RelationTableIndexService(ESCustomTemplate esCustomTemplate) {
        super(esCustomTemplate);
    }


    public void insert(String esIndex, EsIndexConfig.TableEsMapping relationTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        if (!Objects.equals(relationTable.getTableName(), dml.getTable())) {
            logger.error("insert method found relationTable:{}, but not match the dml table:{}", relationTable.getTableName(), dml.getTable());
            return;
        }

        List<Map<String, Object>> esFieldDataList = getRelationTableEsFiledVal(esIndex, relationTable, dml.getData());
        if (esFieldDataList.isEmpty()) {
            return;
        }

        for (Map<String, Object> esFieldData : esFieldDataList) {
            Object resultIdVal = esFieldData.get(relationTable.getEsId());
            if (resultIdVal == null) {
                logger.error("insert error, not found esId in esFieldVal, please check your config");
                continue;
            }
            esFieldData.remove(relationTable.getEsId());

            logger.info("insert data to index:{}, table: {}, id: {}, esFieldData:{}",
                    esIndex, dml.getTable(), resultIdVal, esFieldData);

            // 从表只更新索引的字段
            esCustomTemplate.update(esIndex, resultIdVal, esFieldData);
        }
    }


    public void update(String esIndex, EsIndexConfig.TableEsMapping relationTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }

        if (!Objects.equals(relationTable.getTableName(), dml.getTable())) {
            logger.error("update method found relationTable:{}, but not match the dml table:{}", relationTable.getTableName(), dml.getTable());
            return;
        }

        // 如果修改的字段不是esFields和白名单中的字段，则不需要更新索引
        if (!needUpdateEsIndex(oldList, relationTable)) {
            logger.info("oldList:{}, not in esFields and whiteListFields!", oldList);
            return;
        }

        List<Map<String, Object>> esFieldDataList = getRelationTableEsFiledVal(esIndex, relationTable, dataList);
        if (!esFieldDataList.isEmpty()) {
            for (Map<String, Object> esFieldData : esFieldDataList) {
                Object resultIdVal = esFieldData.get(relationTable.getEsId());
                if (resultIdVal == null) {
                    logger.error("update error, not found esId in esFieldVal, please check your config");
                    continue;
                }
                esFieldData.remove(relationTable.getEsId());

                logger.info(" update data from index:{}, table: {}, id: {}, updateField:{}",
                        esIndex, dml.getTable(), resultIdVal, esFieldData);

                esCustomTemplate.update(esIndex, resultIdVal, esFieldData);
            }
        }

        List<String> updatedRelationFieldList = getUpdatedRelationFieldList(oldList, relationTable);
        // 如果从表的关联字段修改了，则需要更
        if (!updatedRelationFieldList.isEmpty()) {
            logger.info("updatedRelationFieldList:{}", updatedRelationFieldList);
            List<Map<String, Object>> relationEsFieldDataList = getRelationTableEsFiledVal(esIndex, relationTable, oldList);

            for (Map<String, Object> esFieldData : relationEsFieldDataList) {
                Object resultIdVal = esFieldData.get(relationTable.getEsId());
                if (resultIdVal == null) {
                    logger.error("relate update error, not found esId in esFieldVal, please check your config");
                    continue;
                }
                esFieldData.remove(relationTable.getEsId());

                logger.info("relate update data from index:{}, table: {}, id: {}, updateField:{}",
                        esIndex, dml.getTable(), resultIdVal, esFieldData);

                esCustomTemplate.update(esIndex, resultIdVal, esFieldData);
            }
        }

    }


    private List<Object> getEsIdValues(String esIndex, EsIndexConfig.EsMapping esMapping, List<Map<String, Object>> dmlDatas) {
        // 查找esId的sql为空，则表示从dml中获取
        if (StringUtils.isEmpty(esMapping.getSearchEsIdSql())) {
            List<Object> esIdVals = new ArrayList<>();
            Object esIdVal = getValFromDatas(esIndex, esMapping, dmlDatas, esMapping.getEsId());
            if (esIdVal != null) {
                esIdVals.add(esIdVal);
            }
            logger.info("esIdField:{} in dml, esIdValueList:{}", esMapping.getEsId(), esIdVals);
            return esIdVals;
        } else {
            long begin = System.currentTimeMillis();

            List<Object> valueList = new ArrayList<>();
            for (String fieldName : esMapping.getSearchEsIdConditionFields().split(",")) {
                Object value = getValFromDatas(esIndex, esMapping, dmlDatas, fieldName);
                valueList.add(value);
            }

            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(esMapping.getDataSourceKey());
            List esIdVals = (List) Util.sqlRS(dataSource, esMapping.getSearchEsIdSql(), valueList, rs -> {
                List<Object> esIdValList = new ArrayList<>();
               try {
                   while (rs.next()) {
                       esIdValList.add(rs.getObject(1));
                   }
               } catch (Exception e) {
                   logger.error(e.getMessage(), e);
               }
               return esIdValList;
            });

            logger.info("search cost:{} ms, searchEsIdSql:{}, \n valueList:{}, esIdValueList:{}",
                    System.currentTimeMillis() - begin, esMapping.getSearchEsIdSql(), valueList, esIdVals);
            return esIdVals;
        }
    }


    private List<Map<String, Object>> getRelationTableEsFiledVal(String esIndex, EsIndexConfig.TableEsMapping tableEsMapping, List<Map<String, Object>> dmlDatas) {
        EsIndexConfig.EsMapping esMapping = tableEsMapping.getEsMapping();

        List<Object> esIdValueList = getEsIdValues(esIndex, esMapping, dmlDatas);
        if (esIdValueList.isEmpty()) {
            return new ArrayList<>();
        }

        long beginTime = System.currentTimeMillis();
        DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(esMapping.getDataSourceKey());
        List<String> esFields = Arrays.stream(esMapping.getEsFields().split(",")).collect(Collectors.toList());

        String esIdValues = StringUtils.join(esIdValueList, ",");
        String sql = String.format(esMapping.getSql().replace("?", "%s"), esIdValues);
        logger.info("esMapping_sql:{}", sql);

        List esFieldDataList = (List) Util.sqlRS(dataSource, sql, rs -> {
            List<Map<String, Object>> esFieldDatas = new ArrayList<>();
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new HashMap<>();
                    esFieldData.put(esMapping.getEsId(), getValFromRS(esIndex, esMapping.getObjFields(), rs, esMapping.getEsId()));

                    for (String fieldName : esFields) {
                        esFieldData.put(fieldName, getValFromRS(esIndex, esMapping.getObjFields(), rs, fieldName));
                    }
                    esFieldDatas.add(esFieldData);
                }

                if (esFieldDatas.size() == esIdValueList.size()) {
                    return esFieldDatas;
                }

                for (Object esIdVal : esIdValueList) {
                    boolean hasData = false;
                    for (Map<String, Object> esFieldData : esFieldDatas) {
                        Object searchEsIdVal = esFieldData.get(esMapping.getEsId());
                        if (Objects.equals(esIdVal, searchEsIdVal)) {
                            hasData = true;
                            break;

                        }
                    }

                    if (!hasData) {
                        Map<String, Object> esFieldData = new HashMap<>();
                        esFieldData.put(esMapping.getEsId(), esIdVal);
                        for (String fieldName : esFields) {
                            esFieldData.put(fieldName, null);
                        }
                        esFieldDatas.add(esFieldData);
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return esFieldDatas;
        });

        logger.info(" esIndex:{}, table:{}, count:{}, cost:{} ms, found esFieldData:{}",
                esIndex, tableEsMapping.getTableName(), esFieldDataList.size(),
                System.currentTimeMillis() - beginTime, esFieldDataList);

        return esFieldDataList;
    }




    public void delete(String esIndex, EsIndexConfig.TableEsMapping relationTable, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        if (!Objects.equals(relationTable.getTableName(), dml.getTable())) {
            logger.error("delete method found relationTable:{}, but not match the dml table:{}", relationTable.getTableName(), dml.getTable());
            return;
        }

        List<Map<String, Object>> esFieldDataList = getRelationTableEsFiledVal(esIndex, relationTable, dataList);
        if (esFieldDataList.isEmpty()) {
            return;
        }

        for (Map<String, Object> esFieldData : esFieldDataList) {
            Object resultIdVal = esFieldData.get(relationTable.getEsId());
            if (resultIdVal == null) {
                logger.error("delete error, not found esId in esFieldVal, please check your config");
                continue;
            }
            esFieldData.remove(relationTable.getEsId());

            logger.info(" delete method update data from index:{}, table: {}, id: {}, updateField:{}",
                    esIndex, dml.getTable(), resultIdVal, esFieldData);

            esCustomTemplate.update(esIndex, resultIdVal, esFieldData);
        }

    }


}
