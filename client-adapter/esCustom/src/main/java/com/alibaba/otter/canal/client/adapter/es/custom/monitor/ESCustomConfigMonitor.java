package com.alibaba.otter.canal.client.adapter.es.custom.monitor;

import com.alibaba.otter.canal.client.adapter.es.custom.ESCustomAdapter;
import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.es.custom.support.SPIName;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.Util;
import com.alibaba.otter.canal.client.adapter.support.YamlUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ESCustomConfigMonitor {

    private static final Logger logger = LoggerFactory.getLogger(ESCustomConfigMonitor.class);

    private String                adapterName = SPIName.esCustom;

    private ESCustomAdapter esAdapter;


    private FileAlterationMonitor fileMonitor;

    public void init(ESCustomAdapter esAdapter) {
        this.esAdapter = esAdapter;
        File confDir = Util.getConfDirPath(adapterName);
        try {
            FileAlterationObserver observer = new FileAlterationObserver(confDir,
                    FileFilterUtils.and(FileFilterUtils.fileFileFilter(), FileFilterUtils.suffixFileFilter("yml")));
            FileListener listener = new FileListener();
            observer.addListener(listener);
            fileMonitor = new FileAlterationMonitor(3000, observer);
            fileMonitor.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void destroy() {
        try {
            fileMonitor.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class FileListener extends FileAlterationListenerAdaptor {

        @Override
        public void onFileCreate(File file) {
            super.onFileCreate(file);
            try {
                logger.info("---loading new config-----:{}", file.getName());
                // 加载新增的配置文件
                String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                EsIndexConfig config = YamlUtils.ymlToObj(null, configContent, EsIndexConfig.class, null, null);
                if (config != null) {
                    try {
                        config.validate();
                        config.formatValue();
                    } catch (Exception e) {
                        throw new RuntimeException("ERROR Config: " + file.getName() + " " + e.getMessage(), e);
                    }

                    boolean result = esAdapter.addConfig(file.getName(), config);
                    if (result) {
                        logger.info("Add a new es mapping config: {} to canal adapter", file.getName());
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileChange(File file) {
            super.onFileChange(file);
            try {
                logger.info("---start update config-----:{}", file.getName());
                if (esAdapter.getEsIndexConfigMap().containsKey(file.getName())) {
                    // 加载配置文件
                    String configContent = MappingConfigsLoader.loadConfig(adapterName + File.separator + file.getName());
                    if (configContent == null) {
                        onFileDelete(file);
                        return;
                    }
                    EsIndexConfig config = YamlUtils.ymlToObj(null, configContent, EsIndexConfig.class, null, null);
                    if (config == null) {
                        return;
                    }
                    try {
                        config.validate();
                        config.formatValue();
                    } catch (Exception e) {
                        throw new RuntimeException("ERROR Config: " + file.getName() + " " + e.getMessage(), e);
                    }
                    esAdapter.addConfig(file.getName(), config);
                    logger.info("Change a es mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onFileDelete(File file) {
            super.onFileDelete(file);
            try {
                logger.info("---start delete config-----:{}", file.getName());
                if (esAdapter.getEsIndexConfigMap().containsKey(file.getName())) {
                    esAdapter.deleteConfig(file.getName());
                    logger.info("Delete a es mapping config: {} of canal adapter", file.getName());
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
