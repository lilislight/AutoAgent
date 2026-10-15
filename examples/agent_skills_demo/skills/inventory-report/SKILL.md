---
name: inventory-report
description: 分析商品库存，识别缺货风险并提出补货建议。适用于库存检查、补货报告和缺货风险分析。
metadata:
  author: autoagent
  version: "1"
---

# 库存分析

确认数据范围与数据日期。如果用户没有提供数据，调用 inventory_get 获取库存。
不要推测商品数量、日销量或补货周期。

读取 references/report-rules.md，按照其中的规则计算风险。
读取 assets/report-template.md，使用它的结构输出中文报告。

数据不足时，明确列出缺失字段，不计算没有依据的缺货日期。

scripts/calculate.py 提供同样计算规则的参考实现，可按需读取。
