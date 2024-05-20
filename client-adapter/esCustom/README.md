# 索引同步操作指南

## 添加索引

1. kafka-ui增加topic, 格式: {数据库名称}_{数据表名称}
2. Canal-admin添加instance的配置, 找到对应的instance(与数据库同名), 追加topic配置,参考如下:
    - canal.instance.filter.regex={原先的配置},topic
3. 创建索引.json的配置文件, 登录Kibana, 新建索引, 名称:{数据表名}
4. 进入Canal-launcher/conf目录, 找到application.yml文件, 配置对应的instance=topic
5. 进入conf/esCustom目录, 新建{数据表名或者索引名}.yml文件, 配置好索引映射;

