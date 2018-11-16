import os
import pandas as pd


def dt_handle(date):
    return pd.to_datetime(date).strftime('%Y%m%d')


def set(country=None):
    """
    country: list or iterable
    """
    if country is not None:
        if isinstance(country, str):
            country = [
                country,
            ]
        os.environ['sangreal_calendar'] = ','.join(country)