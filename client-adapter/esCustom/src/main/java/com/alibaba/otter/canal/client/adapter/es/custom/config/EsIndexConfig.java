package com.alibaba.otter.canal.client.adapter.es.custom.config;

import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 *  ES索引配置
 */
@Data
public class EsIndexConfig {

    /**
     *  索引名称
     */
    private String esIndex;

    /**
     *  数据表-es映射
     */
    private List<TableEsMapping> tableEsMappings;


    @Data
    public static class TableEsMapping {
        /**
         *  对应instance
         */
        private String destination;
        /**
         *  分组名称
         */
        private String group;
        /**
         *  数据表名
         */
        private String tableName;
        /**
         *  数据库名，主表必填
         */
        private String database;
        /**
         *  是否为主表
         */
        private boolean mainTable;
        /**
         *  es配置
         */
        private EsMapping esMapping;
        /**
         *  关联关系，主表才有
         */
        private List<Relation> relations = new ArrayList<>();


        public String getEsId() {
            return esMapping.getEsId();
        }

        public List<String> getRelationFieldList() {
            List<String> relationFieldList = new ArrayList<>();
            if (!mainTable) {
                if (esMapping.getRelationFields() != null) {
                    relationFieldList.addAll(Arrays.stream(esMapping.getRelationFields().split(",")).collect(Collectors.toList()));
                }
                return relationFieldList;
            }
            if (relations.isEmpty()) {
                return relationFieldList;
            }
            for (Relation relation : relations) {
                relationFieldList.addAll(Arrays.stream(relation.getRelationFields().split(",")).collect(Collectors.toList()));
            }
            return new ArrayList<>(new HashSet<>(relationFieldList));
        }
    }


    @Data
    public static class Relation {
        /**
         *  主表关联 关联表的字段, 多个可以用","分割
         */
        private String relationFields;
        /**
         *  数据源key
         */
        private String dataSourceKey;
        /**
         *  查询sql
         */
        private String sql;

        /**
         *  关联表在es中字段
         */
        private String esFields;
        /**
         *  对象类型字段
         */
        private Map<String, String> objFields = new HashMap<>();
    }



    @Data
    public static class EsMapping {
        /**
         *  es索引字段
         */
        private String esFields;
        /**
         *  白名单字段
         */
        private String whiteListFields;
        /**
         *  关联字段
         */
        private String relationFields;
        /**
         *  对象类型字段
         */
        private Map<String, String> objFields = new HashMap<>();
        /**
         *  esId
         */
        private String esId;
        /**
         *  数据源配置
         */
        private String dataSourceKey;
        /**
         *  sql
         */
        private String sql;
        /**
         *  查找esId的sql
         */
        private String searchEsIdSql;
        /**
         *  找到esId的sql的条件字段
         */
        private String searchEsIdConditionFields;
    }


    public void validate() {
        // 主表校验
        for (TableEsMapping tableEsMapping : tableEsMappings) {
            if (tableEsMapping.getDestination() == null) {
                throw new NullPointerException("destination is null");
            }
            if (tableEsMapping.getGroup() == null) {
                throw new NullPointerException("group is null");
            }
            if (tableEsMapping.getTableName() == null) {
                throw new NullPointerException("tableName is null");
            }
            if (tableEsMapping.isMainTable()) {
                mainTableCheck(tableEsMapping);
            } else {
                relationTableCheck(tableEsMapping);
            }
        }
    }


    private void relationTableCheck(TableEsMapping tableEsMapping) {
        EsMapping esMapping = tableEsMapping.getEsMapping();
        if (esMapping == null) {
            throw new NullPointerException("esMapping is null");
        }
        if (esMapping.getEsId() == null) {
            throw new NullPointerException("esId is null");
        }
        if (esMapping.getEsFields() == null) {
            throw new NullPointerException("esFields is null");
        }
        if (esMapping.getDataSourceKey() == null) {
            throw new NullPointerException("dataSourceKey is null");
        }
        if (esMapping.getSql() == null) {
            throw new NullPointerException("sql is null");
        }
    }


    private void mainTableCheck(TableEsMapping tableEsMapping) {
        if (tableEsMapping.getDatabase() == null) {
            throw new NullPointerException("database is null");
        }
        EsMapping esMapping = tableEsMapping.getEsMapping();
        if (esMapping == null) {
            throw new NullPointerException("esMapping is null");
        }
        if (esMapping.getEsId() == null) {
            throw new NullPointerException("esId is null");
        }
        if (esMapping.getEsFields() == null) {
            throw new NullPointerException("esFields is null");
        }
        if (esMapping.getDataSourceKey() == null) {
            throw new NullPointerException("dataSourceKey is null");
        }

        for (Relation relation : tableEsMapping.getRelations()) {
            if (relation.getRelationFields() == null) {
                throw new NullPointerException("relation->relationFields is null");
            }
            if (relation.getDataSourceKey() == null) {
                throw new NullPointerException("relation->dataSourceKey is null");
            }
            if (relation.getSql() == null) {
                throw new NullPointerException("relation->sql is null");
            }
            if (relation.getEsFields() == null) {
                throw new NullPointerException("relation->esFields is null");
            }
        }
    }



    public void formatValue() {
        for (TableEsMapping tableEsMapping : tableEsMappings) {
            List<String> esFields = Arrays.stream(tableEsMapping.getEsMapping().getEsFields().split(","))
                    .map(String::trim).collect(Collectors.toList());
            tableEsMapping.getEsMapping().setEsFields(String.join(",", esFields));

            if (tableEsMapping.getEsMapping().getWhiteListFields() != null) {
                List<String> whiteListFields = Arrays.stream(tableEsMapping.getEsMapping().getWhiteListFields().split(","))
                        .map(String::trim).collect(Collectors.toList());
                tableEsMapping.getEsMapping().setWhiteListFields(String.join(",", whiteListFields));
            }

            if (tableEsMapping.getEsMapping().getSearchEsIdConditionFields() != null) {
                List<String> searchEsIdFields = Arrays.stream(tableEsMapping.getEsMapping().getSearchEsIdConditionFields().split(","))
                        .map(String::trim).collect(Collectors.toList());
                tableEsMapping.getEsMapping().setSearchEsIdConditionFields(String.join(",", searchEsIdFields));
            }

            if (tableEsMapping.getEsMapping().getRelationFields() != null) {
                List<String> relationFields = Arrays.stream(tableEsMapping.getEsMapping().getRelationFields().split(","))
                        .map(String::trim).collect(Collectors.toList());
                tableEsMapping.getEsMapping().setRelationFields(String.join(",", relationFields));
            }

            for (Relation relation : tableEsMapping.getRelations()) {
                List<String> relationEsFields = Arrays.stream(relation.getEsFields().split(","))
                        .map(String::trim).collect(Collectors.toList());
                relation.setEsFields(String.join(",", relationEsFields));

                List<String> relationFields = Arrays.stream(relation.getRelationFields().split(","))
                        .map(String::trim).collect(Collectors.toList());
                relation.setRelationFields(String.join(",", relationFields));
            }
        }
    }


    public TableEsMapping getTableEsMappingByTableName(String tableName) {
        for (TableEsMapping tableEsMapping : tableEsMappings) {
            if (Objects.equals(tableName, tableEsMapping.getTableName())) {
                return tableEsMapping;
            }
        }
        return null;
    }


    public TableEsMapping getMainTable() {
        for (TableEsMapping tableEsMapping : tableEsMappings) {
            if (tableEsMapping.mainTable) {
                return tableEsMapping;
            }
        }
        return null;
    }


    public TableEsMapping getTableEsMapping(String configKey) {
        for (TableEsMapping tableEsMapping : tableEsMappings) {
            String mappingKey = getConfigKey(tableEsMapping.getDestination(), tableEsMapping.getGroup());
            if (Objects.equals(configKey, mappingKey)) {
                return tableEsMapping;
            }
        }
        return null;
    }

    public TableEsMapping getTableEsMapping(String destination, String group)  {
        String configKey = getConfigKey(destination, group);
        return getTableEsMapping(configKey);
    }


    public static String getConfigKey(String destination, String group) {
        return StringUtils.join(new String[] { destination, group}, '-');
    }

}
