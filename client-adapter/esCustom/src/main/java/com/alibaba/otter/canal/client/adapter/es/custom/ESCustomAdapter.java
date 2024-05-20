package com.alibaba.otter.canal.client.adapter.es.custom;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.custom.config.ESCustomSyncConfigLoader;
import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.es.custom.index.MainTableIndexService;
import com.alibaba.otter.canal.client.adapter.es.custom.index.RelationTableIndexService;
import com.alibaba.otter.canal.client.adapter.es.custom.monitor.ESCustomConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.custom.support.*;
import com.alibaba.otter.canal.client.adapter.support.*;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;

import java.util.stream.Collectors;

/**

import java.util.*;

/**
 *  自定义适配同步器
 */
@SPI("esCustom")
public class ESCustomAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(ESCustomAdapter.class);

    private ESCustomTemplate esCustomTemplate;

    private ESConnection esConnection;

    @Getter
    private Map<String, EsIndexConfig>  esIndexConfigMap;

    private RelationTableIndexService relationTableIndexService;

    private MainTableIndexService mainTableIndexService;

    private ESCustomConfigMonitor esCustomConfigMonitor;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        try {
            logger.info("---start to init esCustom outerAdapter---");
            esIndexConfigMap = ESCustomSyncConfigLoader.load();

            String[] hostArray = configuration.getHosts().split(",");
            esConnection = new ESConnection(hostArray, configuration.getProperties());
            esCustomTemplate = new ESCustomTemplate(esConnection);

            relationTableIndexService = new RelationTableIndexService(esCustomTemplate);
            mainTableIndexService = new MainTableIndexService(esCustomTemplate);

            esCustomConfigMonitor = new ESCustomConfigMonitor();
            esCustomConfigMonitor.init(this);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }

        for (Dml dml : dmls) {
            long begin = System.currentTimeMillis();
            String configKey = EsIndexConfig.getConfigKey(dml.getDestination(), dml.getGroupId());
            logger.info("configKey:{}, dml:{}", configKey, dml);
            Map<String, EsIndexConfig.TableEsMapping> esMappingHashMap = getTableEsMapping(configKey);
            if (!esMappingHashMap.isEmpty()) {
                String esIndex = esMappingHashMap.keySet().iterator().next();
                EsIndexConfig.TableEsMapping tableEsMapping = esMappingHashMap.values().iterator().next();
                try {
                    dispatch(esIndex, tableEsMapping, dml);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    // todo -> 人工介入，手动补偿
                }

            }
            logger.info("configKey:{}, cost:{} ms", configKey, System.currentTimeMillis() - begin);
        }

        esCustomTemplate.commit();
    }

    private void dispatch(String esIndex, EsIndexConfig.TableEsMapping tableEsMapping, Dml dml) {
        String type = dml.getType();
        if (type != null && type.equalsIgnoreCase("INSERT")) {
            if (tableEsMapping.isMainTable()) {
                mainTableIndexService.insert(esIndex, tableEsMapping, dml);
            } else {
                relationTableIndexService.insert(esIndex, tableEsMapping, dml);
            }
        } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
            if (tableEsMapping.isMainTable()) {
                mainTableIndexService.update(esIndex, tableEsMapping, dml);
            } else {
                relationTableIndexService.update(esIndex, tableEsMapping, dml);
            }
        } else if (type != null && type.equalsIgnoreCase("DELETE")) {
            if (tableEsMapping.isMainTable()) {
                mainTableIndexService.delete(esIndex, tableEsMapping, dml);
            } else {
                relationTableIndexService.delete(esIndex, tableEsMapping, dml);
            }
        }
    }


    private Map<String, EsIndexConfig.TableEsMapping> getTableEsMapping(String configKey) {
        HashMap<String, EsIndexConfig.TableEsMapping> esMappingHashMap = new HashMap<>();

        for (EsIndexConfig esIndexConfig : esIndexConfigMap.values()) {
            EsIndexConfig.TableEsMapping tableEsMapping = esIndexConfig.getTableEsMapping(configKey);
            if (tableEsMapping != null) {
                esMappingHashMap.put(esIndexConfig.getEsIndex(), tableEsMapping);
                return esMappingHashMap;
            }
        }
        return esMappingHashMap;
    }


    @Override
    public boolean reIndex(String task, String esId) {
        EsIndexConfig esIndexConfig = esIndexConfigMap.get(task);
        if (esIndexConfig == null) {
            return false;
        }
        try {
            String esIndex = esIndexConfig.getEsIndex();
            EsIndexConfig.TableEsMapping mainTable = esIndexConfig.getMainTable();

            logger.info("reIndex, esIndex:{}, tableName:{}, esId:{}", esIndex, mainTable.getTableName(), esId);
            List<Dml> dmlList = buildDmlBySearchSql(esIndex, mainTable, esId, null);
            for (Dml dml : dmlList) {
                mainTableIndexService.delete(esIndex, mainTable, dml);
                mainTableIndexService.insert(esIndex, mainTable, dml);
                esCustomTemplate.commit();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        logger.info("task:{}, params:{}", task, params);

        EsIndexConfig esIndexConfig = esIndexConfigMap.get(task);
        if (esIndexConfig == null) {
            etlResult.setSucceeded(false);
            etlResult.setErrorMessage("Task not found");
            return etlResult;
        }

        String esIndex = esIndexConfig.getEsIndex();
        EsIndexConfig.TableEsMapping mainTable = esIndexConfig.getMainTable();
        EsIndexConfig.EsMapping esMapping = mainTable.getEsMapping();

        DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(esMapping.getDataSourceKey());

        // 统计主表数据总数
        String countSql = " select count(*) from `" + mainTable.getDatabase() + "`." + mainTable.getTableName();
        int mainTableCount = (Integer) Util.sqlRS(dataSource, countSql, rs -> {
            try {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return 0;
        });
        logger.info("etl sync mainTable:{}, count_sql:{}, count:{}", mainTable.getTableName(), countSql, mainTableCount);

        // todo -> 超过1万条，开多线程, 分批次
        if (mainTableCount < 1000) {}

        String lastId = null;
        List<Dml> dmlList = buildDmlBySearchSql(esIndex, mainTable, null, lastId);
        while (!dmlList.isEmpty()) {
            for (Dml dml : dmlList) {
                mainTableIndexService.insert(esIndex, mainTable, dml);
                esCustomTemplate.commit();
            }
            List<Map<String, Object>> dataList = dmlList.get(dmlList.size() - 1).getData();
            Object esIdVal = mainTableIndexService.getMainTableEsIdVal(esIndex, mainTable, dataList);
            if (esIdVal == null) {
                break;
            }
            lastId = (String) esIdVal;
            dmlList = buildDmlBySearchSql(esIndex, mainTable, null, lastId);
        }

        etlResult.setResultMessage("etl全量同步:" + mainTableCount);
        etlResult.setSucceeded(true);
        return etlResult;
    }


    private List<Dml> buildDmlBySearchSql(String esIndex, EsIndexConfig.TableEsMapping mainTable, String esId, String lastId) {
        EsIndexConfig.EsMapping esMapping = mainTable.getEsMapping();
        String selectSql = " select " + esMapping.getEsFields() + " from `" + mainTable.getDatabase() + "`." + mainTable.getTableName();
        if (esId != null) {
            selectSql += " where " + esMapping.getEsId() + " = " + esId;
        }
        else if (lastId != null) {
            selectSql += " where " + esMapping.getEsId() + " > " + lastId + " order by id asc limit 200 ";
        }

        DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(esMapping.getDataSourceKey());
        List<String> esFields = Arrays.stream(esMapping.getEsFields().split(",")).collect(Collectors.toList());

        logger.info(" buildDmlBySearchSql:{} ", selectSql);

        List dmlList = (List) Util.sqlRS(dataSource, selectSql, rs -> {
            List<Dml> dmls = new ArrayList<>();
            try {
                while (rs.next()) {
                    Dml dml = new Dml();
                    dml.setTable(mainTable.getTableName());
                    List<Map<String, Object>> data = new ArrayList<>();
                    dml.setData(data);

                    for (String esField : esFields) {
                        Object value = mainTableIndexService.getValFromRS(esIndex, esMapping.getObjFields(), rs, esField);
                        Map<String, Object> fieldData = new HashMap<>();
                        fieldData.put(esField, value);
                        data.add(fieldData);
                    }

                    logger.info("found dml:{}", dml.getData());
                    dmls.add(dml);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            return dmls;
        });

        return dmlList;
    }


    @Override
    public Map<String, Object> count(String task) {
        return new HashMap<>();
    }


    @Override
    public void destroy() {
        esCustomConfigMonitor.destroy();
    }


    public boolean addConfig(String name, EsIndexConfig config) {
        esIndexConfigMap.put(name, config);
        return false;
    }


    public void deleteConfig(String name) {
        esIndexConfigMap.remove(name);
    }

}
