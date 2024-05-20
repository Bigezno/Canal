package com.alibaba.otter.canal.client.es.custom.adapter;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.adapter.es.custom.support.ESConnection;
import com.alibaba.otter.canal.client.adapter.es.custom.support.ESCustomTemplate;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.util.Assert;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Ignore
public class ESConnectionTest {

    ESConnection esConnection;

    @Before
    public void init() throws UnknownHostException {
        String[] hosts = new String[]{"http://localhost:9200"};
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", "dev");
        properties.put("security.auth", "elastic:test");
        esConnection = new ESConnection(hosts, properties);
    }

    @Test
    public void test01() {
        MappingMetadata mappingMetaData = esConnection.getMapping("mytest_user");

        Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
        Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
        for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
            Map<String, Object> value = (Map<String, Object>) entry.getValue();
            if (value.containsKey("properties")) {
                System.out.println(entry.getKey() + " object");
            } else {
                System.out.println(entry.getKey() + " " + value.get("type"));
                Assert.notNull(entry.getKey(), "null column name");
                Assert.notNull(value.get("type"), "null column type");
            }
        }
    }

    @Test
    public void test02() {
        ESCustomTemplate customTemplate = new ESCustomTemplate(esConnection);
        Map<String, Object> esFiledData = new HashMap<>();
        String val = "[{\"role_id\": 2, \"role_name\": \"设备管理员\"}]";
        esFiledData.put("role", JSON.parse(val));
        customTemplate.update("t_test_user", "1", esFiledData);
        customTemplate.commit();
    }


}
