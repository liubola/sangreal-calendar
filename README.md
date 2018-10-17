# sangreal_calendar
trade_dt handle package for A-share market, use tushare as datasource

以tushare为数据源的中国交易日历处理包

## 安装

```pip install sangreal_calendar```

## 交易日处理函数

```python
from sangreal_calenadr import *

# 获取交易日列表
get_trade_dt_list(begin_dt='20180101', end_dt='20180201')
['20180102', '20180103', ..., '20180201']
```

```python
# 获取交易日DataFrame
get_trade_dt_list(begin_dt='20180101', end_dt='20180201', astype='pd')
      trade_dt
6613  20180102
6614  20180103
6615  20180104
6616  20180105
6617  20180108
```

```python
# 调整非交易日至之前的最近一个交易日
adjust_trade_dt('20180101')
'20171229'
```

```python
# 调整非交易日至之前的最后一个交易日
adjust_trade_dt('20180101', 'next')
'20180102'
```

```python
# 往后step 1个交易日
# 注意，对于非交易日的输入，会自动将其adjust至之后的最近一个交易日
# 本质上是 step_trade_dt('20180102', 1)
step_trade_dt('20180101', 1)
'20180103'
```

```python
# 往后step 1个交易日
step_trade_dt('20180101', -1)
'20171229'
```

```python
# 计算两个日期间的交易日长度
delta_trade_dt('20180101', '20180305')
40
```

## 时间频率处理函数











