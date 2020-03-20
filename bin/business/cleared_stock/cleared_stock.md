[TOC]

### 一、 sync from hive to hbase

##### 0 hbase表更新时间表
sdp:feed_in_time
rowkey:name_type

|字段名|类型|描述|
|:--|:--|:-----|
|name |String|库名加表名，e.g. hive 的为 db1.name1, hbase 为 ns1:tbl1|
|type |String| 表类型：hbase hive mysql ...|
|feed_in_time |String|本次数据更新的自然时间，格式为 yyyy-MM-dd HH:mm:ss|
|last_data_time| String |azkaban 的 sdp data time 格式为 yyyyMMdd|
|latest_data_time| String | 最大的  last_data_time 格式为 yyyyMMdd|

##### 1 建 hbase 表
因为建 namespace 只有admin才有权限，其他开发者只能在 portal 创建
```sql
# 创建 namespace， 非管理员在portal操作
create_namespace 'clearedstock' // 生产 gulele 没有这个权限，在页面创建

# 创建 table，建议在 portal 操作
create 'clearedstock:rt_cust_cleared_stock','cf'
create 'clearedstock:rt_cust_cleared_stock_detail','cf'

# 修改压缩策略为 snappy，无压缩的话空间大概需要 600G+， 压缩后为 110G+
# 要求，hadoop checknative 必须支持 snappy，并且 spark conf 配置 spark.driver.extraLibraryPath 包含 native
disable 'clearedstock:rt_cust_cleared_stock'
alter 'clearedstock:rt_cust_cleared_stock', NAME => 'cf', COMPRESSION => 'snappy'
enable 'clearedstock:rt_cust_cleared_stock'

disable 'clearedstock:rt_cust_cleared_stock_detail'
alter 'clearedstock:rt_cust_cleared_stock_detail', NAME => 'cf', COMPRESSION => 'snappy'
enable 'clearedstock:rt_cust_cleared_stock_detail'

# 如果之前有数据，手动压缩
major_compact 'clearedstock:rt_cust_cleared_stock'
major_compact 'clearedstock:rt_cust_cleared_stock_detail'

hadoop fs -du -h /apps/hbase/data/data/clearedstock
```
##### 2 创建hadoop权限策略
添加新的hadoop策略（修改之前的策略未生效，待分析）
给相关人员(azkaban user)添加 `/tmp/hbase_write` 的读写权限；这个是我们的 bulkload 目录；

##### 3 指定日期的方法
在 `sync.yaml` 中配置 sync_day 来实现；
- 1 不配置 `sync_day` use azkaban date time
- 2 sync_day: 20200203 表示同步 2020-02-03 这天的数据
- 3 sync_day: -1 表示全量历史数据

##### 4 调度方法
首先，我们使用的是通用包和配偶文件、脚本，因此每一个任务都是用一个子目录；
然后 azkaban 使用 `command` 方式的job。
```bash
sh sub_dir_name/shell.sh
# 另外，shell中第一个命令为进入子目录
cd sub_dir_name
# 然后才是 spark 命令等等
```

##### 5 指标情况