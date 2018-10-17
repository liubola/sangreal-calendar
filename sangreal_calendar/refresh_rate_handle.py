from abc import ABCMeta, abstractmethod
from .trade_dt_handle import get_trade_dt_list, step_trade_dt, adjust_trade_dt
import pandas as pd


class RefreshBase(metaclass=ABCMeta):
    """获取调仓日期的基类
    
    Attributes: 
        *args: -1 or 1, int or (1, -1)
    """

    def __init__(self, *args):
        if abs(sum(args)) > 1:
            raise ValueError('args must be 1 or -1')
        self.args = sorted(args, reverse=True)

    @abstractmethod
    def get_trade_dt_list(self, begin_dt, end_dt):
        pass

    @staticmethod
    def freq_handle(arg, df, step=1):
        if arg == 1:
            tmp_df = df['trade_dt'].apply(
                lambda x: adjust_trade_dt(x[:6] + '01', 'next'))
        elif arg == -1:
            tmp_df = df['trade_dt'].apply(
                lambda x: step_trade_dt(str(int(x[:6]) + step) + '01', -1))
        return tmp_df

    @staticmethod
    def df_handle(begin_dt, end_dt, func):
        begin_dt = begin_dt.strftime('%Y%m%d') if not isinstance(
            begin_dt, str) else begin_dt
        end_dt = end_dt.strftime('%Y%m%d') if not isinstance(end_dt,
                                                             str) else end_dt
        df = get_trade_dt_list(begin_dt, end_dt, astype='pd').copy()

        df['trade_dt'] = df['trade_dt'].apply(func)
        df.drop_duplicates(inplace=True)
        return df


class Monthly(RefreshBase):
    def get_trade_dt_list(self, begin_dt, end_dt):
        df = self.df_handle(begin_dt, end_dt, lambda x: x[:6])
        all_trade_dt = pd.Series()
        for arg in self.args:
            tmp_df = self.freq_handle(arg, df)
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        all_trade_dt = pd.to_datetime(all_trade_dt)
        return all_trade_dt[all_trade_dt >= begin_dt]


class Weekly(RefreshBase):
    def get_trade_dt_list(self, begin_dt, end_dt):
        begin_dt = begin_dt.strftime('%Y%m%d') if not isinstance(
            begin_dt, str) else begin_dt
        end_dt = end_dt.strftime('%Y%m%d') if not isinstance(end_dt,
                                                             str) else end_dt
        df = get_trade_dt_list(
            step_trade_dt(begin_dt, -20),
            step_trade_dt(end_dt, 20),
            astype='pd').copy()
        df['trade_dt'] = pd.to_datetime(df['trade_dt'])
        df['week'] = df['trade_dt'].map(lambda x: f"{x.year}{x.week}")
        all_trade_dt = pd.Series()
        for arg in self.args:
            if arg == 1:
                tmp_df = df.drop_duplicates('week', keep='first')['trade_dt']
            elif arg == -1:
                tmp_df = df.drop_duplicates('week', keep='last')['trade_dt']
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        return all_trade_dt[(all_trade_dt >= begin_dt)
                            & (all_trade_dt <= end_dt)].drop_duplicates()


class BiWeekly(RefreshBase):
    def get_trade_dt_list(self, begin_dt, end_dt):
        all_trade_dt = pd.Series()
        for arg in self.args:
            if arg == 1:
                tmp_df = Weekly(1).get_trade_dt_list(begin_dt, end_dt)[::2]
            elif arg == -1:
                tmp_df = Weekly(-1).get_trade_dt_list(begin_dt, end_dt)[::2]
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        return all_trade_dt.drop_duplicates()


class Quarterly(RefreshBase):
    @staticmethod
    def _quarter(x):
        if x <= x[:4] + '0331':
            return x[:4] + '01'
        elif x <= x[:4] + '0630':
            return x[:4] + '04'
        elif x <= x[:4] + '0930':
            return x[:4] + '07'
        elif x <= x[:4] + '1231':
            return x[:4] + '10'

    def get_trade_dt_list(self, begin_dt, end_dt):
        df = self.df_handle(begin_dt, end_dt, self._quarter)
        all_trade_dt = pd.Series()
        for arg in self.args:
            tmp_df = self.freq_handle(arg, df, 3)
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        all_trade_dt = pd.to_datetime(all_trade_dt)
        return all_trade_dt[all_trade_dt >= begin_dt]


class Reportly(RefreshBase):
    @staticmethod
    def _report(x):
        if x <= x[:4] + '0430':
            return str(int(x[:4]) - 1) + '11'
        elif x <= x[:4] + '0831':
            return x[:4] + '05'
        elif x <= x[:4] + '1031':
            return x[:4] + '09'
        elif x <= x[:4] + '1231':
            return x[:4] + '11'

    def get_trade_dt_list(self, begin_dt, end_dt):
        df = self.df_handle(begin_dt, end_dt, self._report)
        all_trade_dt = pd.Series()
        for arg in self.args:
            if arg == 1:
                tmp_df = df['trade_dt'].apply(
                    lambda x: adjust_trade_dt(x[:6] + '01', 'next'))
            elif arg == -1:

                def neg_report(x):
                    if x[-2:] == '11':
                        return step_trade_dt(str(int(x[:4]) + 1) + '0501', -1)
                    elif x[-2:] == '09':
                        return step_trade_dt(x[:4] + '1101', -1)
                    elif x[-2:] == '05':
                        return step_trade_dt(x[:4] + '0901', -1)

                tmp_df = df['trade_dt'].apply(neg_report)
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        all_trade_dt = pd.to_datetime(all_trade_dt)
        return all_trade_dt[all_trade_dt >= begin_dt]


class Yearly(RefreshBase):
    def get_trade_dt_list(self, begin_dt, end_dt):
        df = self.df_handle(begin_dt, end_dt, lambda x: x[:4] + '01')
        all_trade_dt = pd.Series()
        for arg in self.args:
            tmp_df = self.freq_handle(arg, df, 100)
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        all_trade_dt = pd.to_datetime(all_trade_dt)
        return all_trade_dt[all_trade_dt >= begin_dt]


class Halfyearly(RefreshBase):
    @staticmethod
    def _year(x):
        if x <= x[:4] + '0630':
            return x[:4] + '01'
        elif x <= x[:4] + '1231':
            return x[:4] + '07'

    def get_trade_dt_list(self, begin_dt, end_dt):
        df = self.df_handle(begin_dt, end_dt, self._year)
        all_trade_dt = pd.Series()
        for arg in self.args:
            tmp_df = self.freq_handle(arg, df, 6)
            all_trade_dt = pd.concat([all_trade_dt, tmp_df])
        all_trade_dt.sort_values(inplace=True)
        all_trade_dt = pd.to_datetime(all_trade_dt)
        return all_trade_dt[all_trade_dt >= begin_dt]


if __name__ == '__main__':
    from dateutil.parser import parse
    # print(
    #     Monthly(1, -1).get_trade_dt_list(parse('20080101'), parse('20100101')))

    # print(
    #     Weekly(1, -1).get_trade_dt_list(parse('20080101'), parse('20100101')))

    # print(
    #     Quarterly(1, -1).get_trade_dt_list(
    #         parse('20080101'), parse('20100101')))
    lst = Weekly(-1).get_trade_dt_list(parse('20171229'), parse('20180301'))
    print(lst, type(lst))
