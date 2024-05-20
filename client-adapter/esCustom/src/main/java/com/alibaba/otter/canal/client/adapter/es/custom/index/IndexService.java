package com.alibaba.otter.canal.client.adapter.es.custom.index;

import com.alibaba.otter.canal.client.adapter.es.custom.config.EsIndexConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

public interface IndexService {

    void insert(String esIndex, EsIndexConfig.TableEsMapping tableMapping, Dml dml);

    void update(String esIndex, EsIndexConfig.TableEsMapping tableMapping, Dml dml);

    void delete(String esIndex, EsIndexConfig.TableEsMapping tableMapping, Dml dml);

}
