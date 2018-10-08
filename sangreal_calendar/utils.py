import os


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