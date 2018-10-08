import tushare as ts
import os
import pandas as pd
from itertools import chain


def china_td():
    """获取所有交易日
  
    Returns:
        list of 'trade_dt'
    """
    df = ts.trade_cal()
    df = df[df['isOpen'] == 1][['calendarDate']]
    df.columns = ['trade_dt']
    df['trade_dt'] = df['trade_dt'].map(lambda x: x.replace('-', ''))
    return df.trade_dt.tolist()


def usa_td():
    pass


# 这个要作到说明文档中
dict_td = {
    'cn': china_td,
    'us': usa_td,
}


def mixin_trade_dt():
    country_list = os.environ.get('sangreal_calendar')
    if country_list is not None:
        country_list = country_list.split(',')
    else:
        country_list = ['cn',]
    lst = list(set(chain(*(dict_td[k]() for k in country_list))))
    lst.sort()
    return pd.DataFrame(data=lst, columns=['trade_dt'])
