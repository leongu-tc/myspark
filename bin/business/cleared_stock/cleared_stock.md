[TOC]

### 一、 sync from hive to hbase
##### 1 建 hbase 表
因为建 namespace 只有admin才有权限，其他开发者只能在 portal 创建
```sql
# 创建 namespace， 非管理员在portal操作
create_namespace 'assetanalysis' // 生产 gulele 没有这个权限，在页面创建

# 创建 table，建议在 portal 操作
create 'assetanalysis:rt_cust_daily_return_rate','cf'
create 'assetanalysis:rt_cust_daily_stkreturn','cf'
create 'assetanalysis:rt_cust_return_data','cf'
create 'assetanalysis:rt_cust_month_stk_return','cf'
create 'assetanalysis:rt_cust_stk_rank','cf'

# 修改压缩策略为 snappy，无压缩的话空间大概需要 600G+， 压缩后为 110G+
# 要求，hadoop checknative 必须支持 snappy，并且 spark conf 配置 spark.driver.extraLibraryPath 包含 native
disable 'assetanalysis:rt_cust_daily_return_rate'
alter 'assetanalysis:rt_cust_daily_return_rate', NAME => 'cf', COMPRESSION => 'snappy'
enable 'assetanalysis:rt_cust_daily_return_rate'

disable 'assetanalysis:rt_cust_daily_stkreturn'
alter 'assetanalysis:rt_cust_daily_stkreturn', NAME => 'cf', COMPRESSION => 'snappy'
enable 'assetanalysis:rt_cust_daily_stkreturn'

disable 'assetanalysis:rt_cust_return_data'
alter 'assetanalysis:rt_cust_return_data', NAME => 'cf', COMPRESSION => 'snappy'
enable 'assetanalysis:rt_cust_return_data'

disable 'assetanalysis:rt_cust_month_stk_return'
alter 'assetanalysis:rt_cust_month_stk_return', NAME => 'cf', COMPRESSION => 'snappy'
enable 'assetanalysis:rt_cust_month_stk_return'

disable 'assetanalysis:rt_cust_stk_rank'
alter 'assetanalysis:rt_cust_stk_rank', NAME => 'cf', COMPRESSION => 'snappy'
enable 'assetanalysis:rt_cust_stk_rank'

# 如果之前有数据，手动压缩
major_compact 'assetanalysis:rt_cust_daily_return_rate'
major_compact 'assetanalysis:rt_cust_daily_stkreturn'
major_compact 'assetanalysis:rt_cust_return_data'
major_compact 'assetanalysis:rt_cust_month_stk_return'
major_compact 'assetanalysis:rt_cust_stk_rank'

hadoop fs -du -h /apps/hbase/data/data/assetanalysis
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
- 一次完整的历史数据执行（80核 3G）一共需要 3小时13分钟的时间；
- 一次单日任务数据执行（30核 1G）一共需要 4 分钟时间；
- 压缩前总消耗存储空间：600G，压缩后为 110G左右；