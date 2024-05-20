package com.alibaba.otter.canal.client.adapter.es.custom.config;

import com.alibaba.otter.canal.client.adapter.es.custom.support.SPIName;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.YamlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ES 自定义配置装载器
 */
public class ESCustomSyncConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ESCustomSyncConfigLoader.class);

    private static Map<String, EsIndexConfig> esCustomConfigMap = new HashMap<>();

    private static final AtomicBoolean loading = new AtomicBoolean(false);

    // 强制加载
    public static synchronized Map<String, EsIndexConfig> forceLoad() {
        loading.set(false);
        return load();
    }

    public static synchronized Map<String, EsIndexConfig> load() {
        if (loading.get()) {
            return esCustomConfigMap;
        }

        logger.info("## Start loading es-custom-sync index config ... ");

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs(SPIName.esCustom);
        if (configContentMap.isEmpty()) {
            throw new RuntimeException("please init index configs ");
        }


        configContentMap.forEach((fileName, content) -> {
            EsIndexConfig config = YamlUtils.ymlToObj(null, content, EsIndexConfig.class, null, null);
            if (config == null) {
                throw new RuntimeException("ERROR Config: " + fileName + " the content is:" + content);
            }

            try {
                config.validate();
                config.formatValue();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }

            esCustomConfigMap.put(fileName, config);
        });

        loading.set(true);
        return esCustomConfigMap;
    }

}
