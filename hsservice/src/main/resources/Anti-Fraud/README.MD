1.deviceFingerprint

2.eventHistory
事件历史 key = (partnerId+"_"+riskFlowNo).toUpperCase()
击中策略 key = (partnerId+"_"+riskFlowNo+"_"+strategyId).toUpperCase()
击中规则 key = (partnerId+"_"+riskFlowNo+"_"+ruleId).toUpperCase()
审核数据 key = (partnerId+"_"+riskFlowNo).toUpperCase()
key = MD5Hash.getMD5AsHex(key.getBytes()).substring(0, 8) + key
8.actionHistory



10.dailyRiskReport
出报表：两种方式实现
1）sparkSql直接读取hbase表，计算生成的结果写到新的hbase表
2）sparkSql读取hive表，计算生成的数据写到新的hbase表

11.dailyStrategyReport



3.blackList

4.highRiskList

5.middleRiskList

6.lowRiskList

7.multiPlatformLoan

9.starNetRelation