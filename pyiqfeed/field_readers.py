# coding=utf-8

"""
Functions used to parse individual fields in the feed.

"""

from typing import Union, Tuple
import datetime
import numpy as np
from pyiqfeed.exceptions import UnexpectedField


def blob_to_str(val) -> str:
    """Convert a blob to a string or a blank."""
    if val is None:
        return ""
    else:
        return str(val)


def read_is_market_open(field: str) -> bool:
    """
    Return True if the relevant market is open.

    Markets are often considered closed outside of regular market hours even
    if there is a lot of volume being traded.
    """
    return bool(int(field)) if field != "" else False


def read_is_short_restricted(field: str) -> bool:
    """Return True if the stock cannot be sold short."""
    if field != "":
        if field == 'Y':
            return True
        if field == 'N':
            return False
        else:
            raise UnexpectedField(
                "Unknown Value in Short Restricted Field: %s" % field)
    else:
        return False


def read_tick_direction(field: str) -> np.int8:
    """
    1 if last tick was an uptick, -1 for downtick, 0 for zero-tick.

    Throws a BadField exception if
    """
    if field != "":
        field_as_int = int(field)
        if field_as_int == 173:
            return np.int8(1)
        if field_as_int == 175:
            return np.int8(-1)
        if field_as_int == 183:
            return np.int8(0)
        else:
            raise UnexpectedField(
                "Unknown value in Tick Direction Field: %s" % field)
    else:
        return np.int8(0)


def read_int(field: str) -> int:
    """Read an integer."""
    return int(field) if field != "" else 0


def read_hex(field: str) -> int:
    """Read a hexadecimal integer."""
    return int(field, 16) if field != "" else 0


def read_uint8(field: str) -> np.uint8:
    """Read a uint8."""
    return np.uint8(field) if field != "" else 0


def read_uint16(field: str) -> np.uint16:
    """Read a uint16."""
    return np.uint16(field) if field != "" else 0


def read_uint64(field: str) -> np.uint64:
    """Read a uint64."""
    return np.uint64(field) if field != "" else 0


def read_float(field: str) -> float:
    """Read a float."""
    return float(field) if field != "" else float('nan')


def read_float64(field: str) -> np.float64:
    """Read a float64."""
    return np.float64(field) if field != "" else np.nan


def read_split_string(split_str: str) -> Tuple[np.float64, np.datetime64]:
    """Read a field that encodes the last split date and last split factor."""
    split_fld_0, split_fld_1 = ("", "")
    if split_str != "":
        (split_fld_0, split_fld_1) = split_str.split(' ')
    split_factor = read_float64(split_fld_0)
    split_date = read_mmddccyy(split_fld_1)
    split_data = (split_factor, split_date)
    return split_data


def read_hhmmss_no_colon(field: str) -> int:
    """Read a HH:MM:SS field and return us since midnight."""
    if field != "":
        hour = int(field[0:2])
        minute = int(field[2:4])
        second = int(field[4:6])
        return 1000000 * ((3600 * hour) + (60 * minute) + second)
    else:
        return 0


def read_hhmmss(field: str) -> int:
    """Read a HH:MM:SS field and return us since midnight."""
    if field != "":
        hour = int(field[0:2])
        minute = int(field[3:5])
        second = int(field[6:8])
        return 1000000 * ((3600 * hour) + (60 * minute) + second)
    else:
        return 0


def read_hhmmssmil(field: str) -> int:
    """Read a HH:MM:SS:MILL field and return us since midnight."""
    if field != "":
        hour = int(field[0:2])
        minute = int(field[3:5])
        second = int(field[6:8])
        msecs = int(field[9:])
        return ((1000000 * ((3600 * hour) + (60 * minute) + second)) +
                (1000 * msecs))
    else:
        return 0


def read_hhmmssus(field: str) -> int:
    """Read a HH:MM:SS.us field and return us since midnight."""
    if field != "":
        hour = int(field[0:2])
        minute = int(field[3:5])
        second = int(field[6:8])
        micro = int(field[9:])
        return (1000000 * ((3600 * hour) + (60 * minute) + second)) + micro
    else:
        return 0


def read_mmddccyy(field: str) -> np.datetime64:
    """Read a MM-DD-CCYY field and return a np.datetime64('D') type."""
    if field != "":
        month = int(field[0:2])
        day = int(field[3:5])
        year = int(field[6:10])
        return np.datetime64(
            datetime.date(year=year, month=month, day=day), 'D')
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D')


def read_ccyymmdd(field: str) -> np.datetime64:
    """Read a CCYYMMDD field and return a np.datetime64('D') type."""
    if field != "":
        year = int(field[0:4])
        month = int(field[4:6])
        day = int(field[6:8])
        return np.datetime64(
            datetime.date(year=year, month=month, day=day), 'D')
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D')


def read_timestamp_msg(dt_tm: str) -> Tuple[np.datetime64, int]:
    """Read a CCYYMMDD HH:MM:SS field."""
    if dt_tm != "":
        (date_str, time_str) = dt_tm.split(' ')
        dt = read_ccyymmdd(date_str)
        tm = read_hhmmss(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def read_live_news_timestamp(dt_tm: str) -> Tuple[np.datetime64, int]:
    """Read a CCYYMMDD HH:MM:SS field."""
    if dt_tm != "":
        (date_str, time_str) = dt_tm.split(' ')
        dt = read_ccyymmdd(date_str)
        tm = read_hhmmss_no_colon(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def read_hist_news_timestamp(dt_tm: str) -> Tuple[np.datetime64, int]:
    """Read a news story time"""
    if dt_tm != "":
        date_str = dt_tm[0:8]
        time_str = dt_tm[8:14]
        dt = read_ccyymmdd(date_str)
        tm = read_hhmmss_no_colon(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def read_posix_ts_mil(dt_tm_str: str) -> Tuple[np.datetime64, int]:
    """ Read a POSIX-Date HH:MM:SS:MILL field."""
    if dt_tm_str != "":
        (date_str, time_str) = dt_tm_str.split(" ")
        dt = np.datetime64(date_str, 'D')
        tm = read_hhmmssmil(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def read_posix_ts_us(dt_tm_str: str) -> Tuple[np.datetime64, int]:
    """ Read a POSIX-Date HH:MM:SS:us field."""
    if dt_tm_str != "":
        (date_str, time_str) = dt_tm_str.split(" ")
        dt = np.datetime64(date_str, 'D')
        tm = read_hhmmssus(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def read_posix_ts(dt_tm_str: str) -> Tuple[np.datetime64, int]:
    """Read a POSIX-DATE HH:MM:SS field."""
    if dt_tm_str != "":
        (date_str, time_str) = dt_tm_str.split(" ")
        dt = np.datetime64(date_str, 'D')
        tm = read_hhmmss(time_str)
        return dt, tm
    else:
        return np.datetime64(datetime.date(year=1, month=1, day=1), 'D'), 0


def str_or_blank(val) -> str:
    """Return a string or blank for None."""
    if val is not None:
        return str(val)
    else:
        return ""


def us_since_midnight_to_time(us_dt: Union[int, np.int64]) -> datetime.time:
    """Convert us since midnight to datetime.time with rounding."""
    us = np.int64(us_dt)
    assert us >= 0
    assert us <= 86400000000
    microsecond = us % 1000000
    secs_since_midnight = np.floor(us / 1000000.0)
    hour = np.floor(secs_since_midnight / 3600)
    minute = np.floor((secs_since_midnight - (hour * 3600)) / 60)
    second = secs_since_midnight - (hour * 3600) - (minute * 60)
    return datetime.time(hour=int(hour),
                         minute=int(minute),
                         second=int(second),
                         microsecond=int(microsecond))


def time_to_hhmmss(tm: datetime.time) -> str:
    """Convert a datetime.time to HHMMSS string."""
    if tm is not None:
        return "%.2d%.2d%.2d" % (tm.hour, tm.minute, tm.second)
    else:
        return ""


def datetime64_to_date(dt64: np.datetime64) -> datetime.date:
    """Convert a np.datetime64('D') to a datetime.date"""
    return dt64.astype(datetime.date)


def date_to_yyyymmdd(dt: datetime.date) -> str:
    """Convert a datetime.date to a CCYYMMDD string."""
    if dt is not None:
        return "%.4d%.2d%.2d" % (dt.year, dt.month, dt.day)
    else:
        return ""


def date_us_to_datetime(dt64: np.datetime64,
                        tm_int: Union[int, np.datetime64]) -> datetime.datetime:
    """Convert a np.datetime64('D') and us_since midnight to datetime"""
    dt = datetime64_to_date(dt64)
    tm = us_since_midnight_to_time(tm_int)
    return datetime.datetime(year=dt.year, month=dt.month, day=dt.day,
                             hour=tm.hour, minute=tm.minute, second=tm.second,
                             microsecond=tm.microsecond)


def datetime_to_yyyymmdd_hhmmss(dt_tm: datetime.datetime) -> str:
    """Convert datetime to CCYYMMDD HHMMSS string"""
    if dt_tm is not None:
        return "%.4d%.2d%.2d %.2d%.2d%.2d" % (dt_tm.year,
                                              dt_tm.month,
                                              dt_tm.day,
                                              dt_tm.hour,
                                              dt_tm.minute,
                                              dt_tm.second)
    else:
        return ""
