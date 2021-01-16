from functools import lru_cache

import pandas as pd
from sangreal_calendar.utils import dt_handle


class CalendarBase:
    def __init__(self, dates=None) -> None:
        self._dates = dates

    @property
    def dates(self):
        return self._dates

    def inject(self, dates):
        if isinstance(dates, pd.Series):
            tmp_dates = dates
        else:
            tmp_dates = pd.Series(dates)
        tmp_dates = pd.to_datetime(tmp_dates).dt.strftime('%Y%m%d')
        self._dates = tmp_dates
        return


CALENDAR = CalendarBase()


def get_trade_dts(begin_dt='19900101', end_dt='20990101'):
    """[获取指定时间段的所有交易日]

    Keyword Arguments:
        begin_dt {str or datetime} -- [begin_dt] (default: {'19900101'})
        end_dt {str or datetime} -- [end_dt] (default: {'20990101'})
        astype {str} -- [list or pd] (default: {'list'})

    Raises:
        ValueError -- [f"astype:{astype} must be 'pd' or 'list'!"]

    Returns:
        [pd.DataFrame or list] -- [trade_dts between begin_dt and end_dt]
    """

    tmp_df = CALENDAR.dates.copy()
    begin_dt, end_dt = dt_handle(begin_dt), dt_handle(end_dt)
    tmp_df = tmp_df[(tmp_df >= begin_dt)
                    & (tmp_df <= end_dt)].copy()
    tmp_df.reset_index(drop=True, inplace=True)
    return tmp_df


@lru_cache()
def adjust_trade_dt(date, adjust='last'):
    """[adjust trade_dt]

    Arguments:
        date {[str or datetime]} -- [date]

    Keyword Arguments:
        adjust {str} -- [last or next] (default: {'last'})

    Raises:
        ValueError -- [f"adjust:{adjust} must be 'last' or 'next'!"]

    Returns:
        [str] -- [adjusted trade_dt with %Y%m%d type]
    """

    t_df = CALENDAR.dates.copy()
    date = dt_handle(date)
    if adjust == 'last':
        t_df = t_df[t_df <= date]
        return t_df.iloc[-1]
    elif adjust == 'next':
        t_df = t_df[t_df >= date]
        return t_df.iloc[0]
    else:
        raise ValueError(f"adjust:{adjust} must be 'last' or 'next'!")


@lru_cache()
def step_trade_dt(date, step=1):
    """[step trade_dt]

    Arguments:
        date {[str or datetime]} -- [date]

    Keyword Arguments:
        step {int} -- [step] (default: {1})

    Returns:
        [str] -- [date with %Y%m%d type]
    """

    t_df = CALENDAR.dates.copy()
    date = dt_handle(date)
    if step >= 0:
        try:
            return t_df[t_df >= date].iloc[step]
        except IndexError:
            return t_df.iloc[-1]
    elif step < 0:
        try:
            return t_df[t_df < date].iloc[step]
        except IndexError:
            return t_df.iloc[0]


@lru_cache()
def delta_trade_dt(begin_dt, end_dt):
    """[get length of trade_dt, include begin_dt and end_dt]

    Arguments:
        begin_dt {[str or datetime]} -- [begin_dt]
        end_dt {[tstr or datetime]} -- [end_dt]

    Returns:
        [int] -- [len of date_range]
    """

    t_df = CALENDAR.dates.copy()
    begin_dt, end_dt = dt_handle(begin_dt), dt_handle(end_dt)
    return len(
        t_df[(t_df >= begin_dt) & (t_df <= end_dt)])


if __name__ == "__main__":
    pass