业务数据的存储：
1.deviceFingerprint（设备指纹数据）

2.eventHistory
事件历史 key = (partnerId+"_"+riskFlowNo).toUpperCase()
击中策略 key = (partnerId+"_"+riskFlowNo+"_"+strategyId).toUpperCase()
击中规则 key = (partnerId+"_"+riskFlowNo+"_"+ruleId).toUpperCase()
审核数据 key = (partnerId+"_"+riskFlowNo).toUpperCase()
key = MD5Hash.getMD5AsHex(key.getBytes()).substring(0, 8) + key
3.actionHistory（行为历史的数据）

备注：设备指纹和行为历史的数据存hdfs，事件历史相关的数据存hbase。
      事件历史相关的数据因为需要用到多字段检索数据，可以搭建phoenix平台去hbase检索数据。

出报表：
1.dailyRiskReport（每日风险报表）

2.dailyStrategyReport（每日策略报表）

3.monthReport（月报表）（每月调用量相关的数据，通过，拒绝，审核相关的量等）

备注：出报表两种方式实现，spark定时跑批的方式生成
1）sparkSql直接读取hbase表，计算生成的结果写到新的hbase表
2）sparkSql读取hive表，计算生成的数据写到新的hbase表

名单数据：
1.blackList（黑名单）

2.highRiskList（高风险）

3.middleRiskList（中风险）

4.lowRiskList（低风险）

备注：利用spark跑批分合作方去清洗数据，结果写到hbase

多合作方数据整合：
1.multiPlatformLoan（多头）

2.dataPush（数据推送）

3.starNetRelation（星网关联）（一级关联，二级关联）
身份证关联手机号，身份证关联设备号，设备号关联手机号，设备号关联身份证号等

4.complexRelationalNetwork（复杂关系网络）
多维度的数据去做关联分析，包含历史数据，最新数据，数据历史情况，修改情况

备注：这些都可以利用spark GraphX（图计算）去解决，待研究！

标签数据：
1.用户画像（整合之前所有的数据）
设备指纹，事件历史，行为历史，名单数据，多头（以身份证为中心维度去关联相关标签）

风险数据：
1.风险地图
2.用户地图

备注：使用flink流计算的方式，利用窗口程序，每隔一定的时间统计一次，统计的结果写到redis，供业务系统取数。

我写了数据采集系统，数据仓库，实时分析系统，推荐系统，用户画像