from fastcache import lru_cache

from .datasource import mixin_trade_dt


def df_handle(date):
    return date if isinstance(date, str) else date.strftime('%Y%m%d')


@lru_cache()
def get_all_trade_dt():
    """获取所有交易日
  
    Returns:
        dataframe with columns of 'trade_dt'
    """
    # df = ts.trade_cal()
    # df = df[df['isOpen'] == 1][['calendarDate']]
    # df.columns = ['trade_dt']
    # df['trade_dt'] = df['trade_dt'].map(lambda x: x.replace('-', ''))
    return mixin_trade_dt()


@lru_cache()
def get_trade_dt_list(begin_dt=None, end_dt=None, astype='list'):
    """获取指定时间段的所有交易日

    Args: 
        begin_dt: begin_dt %Y%m%d or datetime.
        end_dt: end_dt %Y%m%d.
        astype: output type, dataframe or list, 'pd' or 'list'
    
    Returns:
        dataframe with columns of 'trade_dt' or list
    """
    df = get_all_trade_dt()
    tmp_df = df.copy()
    if begin_dt is not None:
        tmp_df = tmp_df[tmp_df['trade_dt'] >= begin_dt]
    if end_dt is not None:
        tmp_df = tmp_df[tmp_df['trade_dt'] <= end_dt]
    if astype == 'pd':
        return tmp_df
    elif astype == 'list':
        return tmp_df['trade_dt'].tolist()
    else:
        raise ValueError(f"astype:{astype} must be 'pd' or 'list'!")


@lru_cache()
def adjust_trade_dt(date, adjust='last'):
    """调整自然日至最近的交易日
    
    Args:
        date: date %Y%m%d or datetime.
        adjust: 'last' or 'next'.

    Returns:
        adjust type of trade_dt %Y%m%d
    """
    t_df = get_all_trade_dt()
    date = df_handle(date)
    if adjust == 'last':
        t_df = t_df[t_df['trade_dt'] <= date]
        return t_df['trade_dt'].iloc[-1]
    elif adjust == 'next':
        t_df = t_df[t_df['trade_dt'] >= date]
        return t_df['trade_dt'].iloc[0]
    else:
        raise ValueError(f"adjust:{adjust} must be 'last' or 'next'!")


@lru_cache()
def step_trade_dt(date, step=1):
    """获得n日前（后）的交易日，对于非交易日来说，将其视为之后的第一个交易日
    Args:
        date: %Y%m%d or datetime.
        step: int.

    Returns:
        date with step %Y%m%d
    """
    t_df = get_all_trade_dt()
    date = df_handle(date)
    if step >= 0:
        t_df = t_df[t_df['trade_dt'] >= date]
    elif step < 0:
        t_df = t_df[t_df['trade_dt'] < date]
    return t_df['trade_dt'].iloc[step]


@lru_cache()
def delta_trade_dt(begin_dt, end_dt):
    """获得指定时间区间内的交易日长度，首位均包含
    Args:
        begin_dt: %Y%m%d or datetime.
        end_dt: %Y%m%d or datetime.

    Returns:
        len of date_range, int.
    """
    t_df = get_all_trade_dt()
    begin_dt, end_dt = df_handle(begin_dt), df_handle(end_dt)
    return len(
        t_df[(t_df['trade_dt'] >= begin_dt) & (t_df['trade_dt'] <= end_dt)])


if __name__ == "__main__":
    print(step_trade_dt('20180607', 1), '20180608')
    # 注意20171231为非交易日，自动视为20180102
    print(step_trade_dt('20171231', 1), '20180103')
    print(delta_trade_dt('20180102', '20180103'))
