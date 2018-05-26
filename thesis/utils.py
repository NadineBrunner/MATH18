from contextlib import contextmanager
from statsmodels.tsa.stattools import grangercausalitytests
import dateutil.parser
import pytz
import sys


def hour_from_string(date_str):
    """Parse string with date and time and return a datetime object
    rounded to the hour.
    """
    date = dateutil.parser.parse(date_str)
    if date.tzinfo:
        date = pytz.utc.normalize(date).replace(tzinfo=None)
    return date.replace(microsecond=0, second=0, minute=0)


def day_from_string(date_str):
    """Parse string with date and time and return a datetime object
    rounded to the day.
    """
    date = dateutil.parser.parse(date_str)
    if date.tzinfo:
        date = pytz.utc.normalize(date).replace(tzinfo=None)
    return date.replace(microsecond=0, second=0, minute=0, hour=0)


def granger(series_a, series_b, output_fio, maxlag):
    """Run a granger causality test and redirect stdout to the output_fio
    because the grangercausalitytests function prints the result to stdout
    but we want to store it in a file.
    """
    with capture_stdout(output_fio):
        data = list(zip(series_a, series_b))
        return grangercausalitytests(data, maxlag=maxlag)


@contextmanager
def capture_stdout(stdout):
    ori_stdout = sys.stdout
    if stdout is not None:
        sys.stdout = stdout
    try:
        yield
    finally:
        if stdout is not None:
            sys.stdout = ori_stdout
