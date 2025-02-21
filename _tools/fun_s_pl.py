# -*- coding: utf-8 -*-
import datetime
import json
from functools import partial
from typing import Any, Callable
from urllib import parse

import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs
import urllib3

# Some display settings for numpy Array, Pandas and Polars DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)
pl.Config.set_fmt_str_lengths(80)
pl.Config.set_tbl_cols(-1)


def cp(s: Any = '', /, display: int = 0, fg: int = 39, bg: int = 48) -> str:
    """
    Return the string for color print in the (IPython) console

    Parameters
    ----------
    s : Any, default=''
    display (显示方式) : int, default=0
        - 0: 默认值
        - 1: 高亮
        - 2: 模糊效果
        - 3: 斜体
        - 4: 下划线
        - 5: 闪烁
        - 7: 反显
        - 8: 不显示（隐藏效果）
        - 9: 划掉字体
        - 22: 非粗体
        - 24: 非下划线
        - 25: 非闪烁
        - 27: 非反显
    fg (前景色) : int, default=39
        - 30: 黑色
        - 31: 红色
        - 32: 绿色
        - 33: 黄色
        - 34: 蓝色
        - 35: 洋红
        - 36: 青色
        - 37: 白色
        - 38: 删除效果并终止
    bg (背景色) : int, default=48
        - 40: 黑色
        - 41: 红色
        - 42: 绿色
        - 43: 黄色
        - 44: 蓝色
        - 45: 洋红
        - 46: 青色
        - 47: 白色

    Returns
    -------
    str
        A string for color print in the console

    Notes
    -----
    stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
    """
    return f'\033[{display};{fg};{bg}m{s}\033[0m'


def print_dict(d: dict, /) -> None:
    """
    Customised function for printing a dictionary nicely on the console

    Parameters
    ----------
    d : dict
        A python dictionary object.

    Returns
    -------
    None
    """
    for k, v in d.items():
        print(cp(cp(f'\n{k}: {type(v)}\n', fg=34, display=4), display=1) + cp(f'\n{v}\n'))


def is_numeric(x: Any, /) -> bool:
    """
    Is `x` numeric?

    Parameters
    ----------
    x : Any
        An object.

    Returns
    -------
    bool
        `True` (is numeric) or `False` (not numeric).
    """
    return isinstance(x, (int, float, complex)) and not isinstance(x, bool)


def _ts_valid_pd(ts: Any, /) -> 'str | None':
    """Validate the input time series: `None` returned as passed"""
    if not isinstance(ts, (pd.Series, pd.DataFrame)):
        return '`ts` must be either pandas.Series or pandas.DataFrame!'
    if not (
            all(isinstance(i, (datetime.datetime, datetime.date)) for i in ts.index)
            or pd.api.types.is_datetime64_any_dtype(ts.index)
    ):
        return f'Wrong dtype in the index: `{ts.index.dtype}` detected!'
    if not (ts.index.size == ts.index.unique().size):
        return '`ts.index` must be unique!'
    if not all(ts.index == ts.index.sort_values(ascending=True)):
        return '`ts.index` must be in chronicle order!'
    if isinstance(ts, pd.DataFrame):
        if ts.shape[1] < 1:
            return 'No column exists in the DataFrame `ts`!'
        df = ts.select_dtypes(include=np.number)
        if not (df.shape[1] == ts.shape[1]):
            return 'All columns in `ts` must be numeric!'
        return None
    if not pd.api.types.is_any_real_numeric_dtype(ts):
        return 'The Series must contain real numbers!'


def _ts_valid_pl(ts: Any, /) -> 'str | None':
    """Validate the input time series: `None` returned as valid"""
    if isinstance(ts, pl.DataFrame):
        if ts.width < 2:
            return '`ts` must have one datetime column and the rest of numeric column(s)!'
        if len(col_dt := ts.select(cs.temporal()).columns) != 1:
            return '`ts` must have one datetime column!'
        if ts[col_dt[0]].unique().len() != ts[col_dt[0]].len():
            return f'The values in the temporal column {col_dt} must be unique!'
        if not ts.sort(by=col_dt, descending=False).equals(ts):
            return f'Column {col_dt} must be sorted in chronicle order!'
        if ts.width != ts.select(cs.numeric()).width + 1:
            return f'Apart from column {col_dt}, the rest column(s) must be numeric!'
        return None
    return '`ts` must be a polars.DataFrame!'


def ts_step(ts: pl.DataFrame, minimum_time_step_in_second: int = 60) -> 'int | None':
    """
    Identify the temporal resolution (in seconds) for a time series

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars time series - 1st column as date/datetime, and other column(s) as numeric
    minimum_time_step_in_second : int, default=60
        The minimum threshold of the time step that can be identified.

    Raises
    ------
    TypeError
        When `_ts_valid_pd(ts)` returns a string.

    Returns
    -------
    int | None
        * **`-1`**: time series is not in a regular time step.
        * Any integer **above `0`**: time series is regular (step in secs).
        * **`None`**: contains no values or a single value.
    """
    if err_str := _ts_valid_pl(ts):
        raise TypeError(err_str)
    col_dt = ts.select(cs.temporal()).columns[0]
    col_v = ts.select(cs.numeric()).columns
    x = ts.fill_nan(None).filter(~pl.all_horizontal(pl.col(col_v).is_null()))
    if len(x) in {0, 1}:
        return None
    diff_in_second = x.select(pl.col(col_dt)).to_series().diff(1).dt.total_seconds()[1:]
    step_min = diff_in_second.filter(diff_in_second >= minimum_time_step_in_second).min()
    return int(step_min) if (diff_in_second % step_min == 0).all() else -1


def is_ts_daily(ts: pl.DataFrame, /) -> bool:
    """Check if a time series (in Polars DataFrame) is daily (day starts at 0 o'clock)"""
    if err_str := _ts_valid_pl(ts):
        raise TypeError(err_str)
    col_dt = ts.select(cs.temporal()).columns[0]
    if not pl.Date.is_(ts[col_dt].dtype):
        time_no_hms = all([
            ts[col_dt].dt.hour().eq(0).all(),
            ts[col_dt].dt.minute().eq(0).all(),
            ts[col_dt].dt.second().eq(0).all(),
        ])
        return (ts_step(ts) == 86400) and time_no_hms
    return True


def ts_pd2pl(ts: 'pd.Series | pd.DataFrame') -> pl.DataFrame:
    """Convert the timeseries from Pandas DataFrame to Polars DataFrame"""
    if (err_str := _ts_valid_pd(ts)) is None:
        print('TimeSeries: Pandas DataFrame -> Polars DataFrame!')
        ts_pl = pl.DataFrame(pd.DataFrame(ts).reset_index()).fill_nan(None)
        col_dt = ts_pl.select(cs.temporal()).columns[0]
        if is_ts_daily(ts_pl):
            ts_pl = ts_pl.with_columns(pl.col(col_dt).cast(pl.Date).alias(col_dt))
        return ts_pl.sort(col_dt)
    raise TypeError(err_str)


def ts_pl2pd(ts: pl.DataFrame) -> pd.DataFrame:
    """Convert the timeseries from Polars DataFrame to Pandas DataFrame"""
    if (err_str := _ts_valid_pl(ts)) is None:
        print('TimeSeries: Polars DataFrame -> Pandas DataFrame!')
        return ts.to_pandas().set_index(ts.select(cs.temporal()).columns[0])
    raise TypeError(err_str)


def na_ts_insert(ts: pl.DataFrame) -> pl.DataFrame:
    """
    Pad Null value into a valid time series (Polars DataFrame)

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars DataFrame - 1st column as date/datetime, and rest column(s) as numeric

    Returns
    -------
    pl.DataFrame
        The Null-padded DataFrame.

    Notes
    -----
        As for irregular time series, The empty-numeric-row-removed DataFrame returned.
    """
    col_dt = ts.select(cs.temporal()).columns[0]
    col_v = ts.select(cs.numeric()).columns
    r = ts.lazy().fill_nan(None).filter(~pl.all_horizontal(pl.col(col_v).is_null()))
    if (step := ts_step(ts)) in {-1, None}:
        return r.sort(col_dt).collect()
    s, e = (
        r.select(
            pl.col(col_dt).min().alias('s'),
            pl.col(col_dt).max().alias('e'),
        )
        .collect()
        .row(0)
    )
    dt_col: pl.Expr = (
        pl.date_range(s, e, f'{int(step / 86400)}d')
        if pl.Date.is_(ts[col_dt].dtype) else
        pl.datetime_range(s, e, f'{step}s')
    )
    dt: pl.LazyFrame = pl.LazyFrame().with_columns(dt_col.alias(col_dt))
    return dt.join(r, on=col_dt, how='left').sort(col_dt).collect()


def hourly_2_daily(
        hts: pl.DataFrame,
        day_starts_at: int = 0,
        agg: Callable = pl.mean,
        prop: float = 1.,
) -> pl.DataFrame:
    """
    Aggregate the hourly time series to daily time series using customised function

    Parameters
    ----------
    hts : pl.DataFrame
        An hourly time series (for a single site)
    day_starts_at : int, optional, default=0
        What time (hour) a day starts - 0 o'clock by default.
        e.g., 9 means the output of daily time series by 9 o'clock!
    agg : Callable, optional, default=pl.mean
        Customised aggregation function (from Polars) - mean by default (`pl.mean`)
    prop : float, optional, default=1
        The ratio of the available data (within a day range)

    Returns
    -------
    pl.DataFrame
        A daily time series (pl.DataFrame) with an extra column of site name

    Raises
    ------
    ValueError
        `day_starts_at` should be an integer between 0 and 23. Error raised otherwise.
    ValueError
        `prop` should be a float in [0, 1]. Error raised otherwise.
    """
    if not isinstance(day_starts_at, int) or day_starts_at < 0 or day_starts_at > 23:
        raise ValueError('`day_starts_at` must be an integer in [0, 23]!\n')
    if prop < 0 or prop > 1:
        raise ValueError('`prop` must be in [0, 1]!\n')
    col_dt = hts.select(cs.temporal()).columns[0]
    col_v = hts.select(cs.numeric()).columns[0]
    r = (
        hts.lazy()
        .select(col_dt, col_v)
        .fill_nan(None)
        .select(
            pl.col(col_dt)
            .sub(datetime.timedelta(seconds=3600 * (1 + day_starts_at)))
            .dt.date()
            .alias('Date'),
            pl.col(col_v),
        )
        .with_columns(pl.col(col_v).count().over('Date').truediv(24).alias('Prop'))
        .filter(pl.col('Prop').ge(prop))
        .drop_nulls(subset=col_v)
        .group_by('Date', maintain_order=True)
        .agg(agg(col_v).alias(f'Agg_{agg.__name__}'))
    )
    return r.collect().pipe(na_ts_insert).with_columns(pl.lit(col_v).alias('Site'))


def ts_info(ts: pl.DataFrame) -> 'pl.DataFrame | None':
    """
    Obtain the data availability of the input time series (Polars DataFrame)

    Parameters
    ----------
    ts : pl.DataFrame
        A Polars input time series.

    Returns
    -------
    pl.DataFrame | None
        * Info on ['Site', 'Start', 'End', 'Length_yr', 'Completion_%'].
        * As for time series of the irregular time steps, 'Completion_%' is ignored.
        * `None` returned when there is no data in the input time series.
    """
    if (con := ts_step(ts)) is None:
        return None
    col_dt = ts.select(cs.temporal()).columns
    col_rest = ts.select(pl.exclude(col_dt)).columns
    col_rest_ = [f'{i}_' for i in col_rest]
    seconds_year = (days_year := 365.2422) * 24 * 3600
    info = (
        ts.lazy()
        .rename(dict(zip(col_rest, col_rest_)))
        .unpivot(on=col_rest_, index=col_dt, variable_name='Site', value_name='V')
        .filter(pl.col('V').fill_nan(None).is_not_null())
        .group_by('Site', maintain_order=True).agg(
            pl.col(col_dt).min().alias('Start'),
            pl.col(col_dt).max().alias('End'),
            pl.col('V').len().alias('n'),
        )
        .with_columns(
            pl.col('End')
            .sub(pl.col('Start'))
            .dt.total_seconds()
            .truediv(seconds_year)
            .alias('Length_yr')
        )
    )
    info = (
        pl.LazyFrame({'Site': col_rest_})
        .join(info, on='Site', how='left', coalesce=True)
        .with_columns(Site=pl.Series(col_rest))
    )
    if con == -1:
        return info.drop('n').collect()
    step_day = con / 86400
    return (
        info.with_columns(
            (pl.col('Length_yr') * days_year + step_day).alias('N'),
            (pl.col('Length_yr') + step_day / days_year),
        )
        .with_columns((pl.col('n') * step_day / pl.col('N') * 100).alias('Completion_%'))
        .drop(['n', 'N'])
        .collect()
    )


def get_AQ(
        url: str,
        basic_auth: str = 'api-read:PR98U3SKOczINoPHo7WM',
        **kwargs
) -> urllib3.response.BaseHTTPResponse:
    """Connect ORC's AQ using 'GET' verb"""
    http = urllib3.PoolManager()
    hdr = urllib3.util.make_headers(basic_auth=basic_auth)
    return http.request('GET', url=url, headers=hdr, **kwargs)


def get_uid(measurement: str, site: str) -> 'str | None':
    """
    Get UniqueId <- f'{measurement}@{site}'

    Parameters
    ----------
    measurement : str
        The format of {Parameter}.{Label}, such as:
            * Flow.WMHourlyMean
            * Discharge.MasterDailyMean
    site : str
        The {LocationIdentifier} behind a site name, such as:
            * WM0062
            * FA780

    Returns
    -------
    str | None
        * str: UniqueId str used for requesting time series (Aquarius)
        * `None`: the UniqueId cannot be located
    """
    if not site.strip():
        raise ValueError("Provide a correct string value for 'Site'!\n")
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url_desc = f'{end_point}/GetTimeSeriesDescriptionList'
    ms = f'{measurement}@{site}'
    parameter, _ = measurement.split('.')
    query_dict = {'LocationIdentifier': site, 'Parameter': parameter}
    r = get_AQ(url=url_desc, fields=query_dict)
    if not (ld := json.loads(r.data.decode('utf-8')).get('TimeSeriesDescriptions')):
        return None
    j_list = [i for i, v in enumerate(ld) if v['Identifier'] == ms]
    return ld[j_list[0]].get('UniqueId', None) if j_list else None


def get_site_name(site) -> 'str | None':
    """Get the site name from Aquarius (for a plate specified as a string in `site`)"""
    if not site.strip():
        raise ValueError("Provide a correct string value for 'Site'!\n")
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url_ldl = f'{end_point}/GetLocationDescriptionList'
    query_dict = {'LocationIdentifier': site}
    r = get_AQ(url=url_ldl, fields=query_dict)
    if not (ld := json.loads(r.data.decode('utf-8')).get('LocationDescriptions')):
        return print(f'\nThe site name for plate [{site}] is NOT found!!!!\n')
    return ld[0].get('Name')


def get_url_AQ(
        measurement: str,
        site: str,
        date_start: int = None,
        date_end: int = None
) -> 'str | None':
    """
    Generate the url for requesting time series

    Parameters
    ----------
    measurement : str
        The format of {Parameter}.{Label}, such as:
            * Flow.WMHourlyMean
            * Discharge.MasterDailyMean
    site : str
        The {LocationIdentifier} behind a site name, such as:
            * WM0062
            * FA780
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.

    Returns
    -------
    str | None
        A string of the url for requesting time series.
    """
    if (uid := get_uid(measurement, site)) is None:
        return None
    fmt = '%Y-%m-%dT00:00:00.0000000+12:00'
    ds = '1800-01-01T00:00:00.0000000+12:00' if date_start is None else (
        datetime.datetime.strptime(f'{date_start}', '%Y%m%d').strftime(fmt))
    de = (
        datetime.datetime.now() + datetime.timedelta(days=1) if date_end is None else
        datetime.datetime.strptime(f'{date_end}', '%Y%m%d') + datetime.timedelta(days=1)
    ).strftime(fmt)
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    query_dict = {
        'TimeSeriesUniqueId': uid,
        'QueryFrom': ds,
        'QueryTo': de,
        'GetParts': 'PointsOnly',
    }
    q_str = parse.urlencode(query_dict)
    return f'{end_point}/GetTimeSeriesCorrectedData?{q_str}'


def get_ts_AQ(
        measurement: str,
        site: str,
        date_start: int = None,
        date_end: int = None
) -> pl.DataFrame:
    """Get the time series for a single site specified by those defined in `get_url_AQ`"""
    empty_df = pl.DataFrame(schema={'Site': str, 'Timestamp': str, 'Value': float})
    if (url := get_url_AQ(measurement, site, date_start, date_end)) is None:
        print(f'\n[{measurement}@{site}] -> No data available for [{site}]!\n')
        return empty_df
    r = get_AQ(url=url)
    if not (ld := json.loads(r.data.decode('utf-8')).get('Points', None)):
        print(f'[{measurement}@{site}] -> No data over the chosen period!\n')
        return empty_df
    return pl.DataFrame(
        {'Site': site} | (
            pl.DataFrame(ld)
            .unnest('Value')
            .rename({'Numeric': 'Value'})
            .with_columns(
                pl.col('Timestamp').cast(str),
                pl.col('Value').cast(float),
            )
            .to_dict()
        )
    )


_HWU_AQ = partial(get_ts_AQ, 'Flow.WMHourlyMean')
_DWU_AQ = partial(get_ts_AQ, 'Abstraction Volume.WMDaily')
_DFlo_AQ = partial(get_ts_AQ, 'Discharge.MasterDailyMean')


def clean_24h_datetime(shit_datetime: str) -> str:
    """
    Clean the 24:00:00 in a datetime string to a normal datetime string

    Parameters
    ----------
    shit_datetime : str
        The first 19 characters for the input follow a format of '%Y-%m-%dT%H:%M:%S',
        and it is supposed to have shit (24:MM:SS) itself, like '2020-12-31T24:00:00'

    Returns
    -------
    str (length of 19)
        A normal datetime string:
            such as '2021-01-01T00:00:00' converted from the shit one mentioned above.
    """
    if not isinstance(shit_datetime, str):
        return None
    date_str, time_str = (s19 := shit_datetime[:19]).split('T')
    *Ymd, H, M, S = [int(i) for i in (date_str.split('-') + time_str.split(':'))]
    return (
            datetime.datetime(*Ymd, H - 1, M, S)
            + datetime.timedelta(hours=1)
    ).strftime('%Y-%m-%dT%H:%M:%S') if H > 23 else s19


def hourly_WU_AQ(
        site_list: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
) -> pl.DataFrame:
    """
    A wrapper of getting hourly rate for multiple water meters (from Aquarius)

    Parameters
    ----------
    site_list : str | list[str]
        A list of water meters' names
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.
    raw_data : bool, optional, default=False
        Whether return the raw data (in l/s) or not (in m^3/s) from Aquarius.

    Returns
    -------
    pl.DataFrame
        A DataFrame of hourly abstraction
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    site_list = list(dict.fromkeys(site_list))
    ts_l = pl.concat([_HWU_AQ(i, date_start, date_end) for i in site_list], how='vertical')
    if raw_data:
        print('\nNote: The (raw) hourly rate of take is in L/s!!!!\n')
        return ts_l
    ts_e = pl.DataFrame(schema={'Time': pl.Datetime} | {i: pl.Float64 for i in site_list})
    ts_w = ts_l.with_columns(
        pl.col('Timestamp')
        .map_elements(clean_24h_datetime, return_dtype=pl.String)
        .str.to_datetime('%Y-%m-%dT%H:%M:%S')
        .alias('Time'),
        pl.col('Value').cast(pl.Float64).truediv(1e3).name.keep(),
    ).pivot(on='Site', index='Time', values='Value')
    print('\nNote: The hourly rate of take is in m^3/s!!!!\n')
    return pl.concat([ts_e, ts_w], how='diagonal').sort('Time').pipe(na_ts_insert)


def daily_WU_AQ(
        site_list: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
) -> pl.DataFrame:
    """
    A wrapper of getting daily rate for multiple water meters (from Aquarius)

    Parameters
    ----------
    site_list : str | list[str]
        A list of water meters' names
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.
    raw_data : bool, optional, default=False
        Whether return the raw data (daily volume in m^3) or not (in m^3/s) from Aquarius.

    Returns
    -------
    pl.DataFrame
        A DataFrame of daily abstraction
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    site_list = list(dict.fromkeys(site_list))
    ts_l = pl.concat([_DWU_AQ(i, date_start, date_end) for i in site_list], how='vertical')
    if raw_data:
        print('\nNote: The (raw) daily take volume is in m^3!!!!\n')
        return ts_l
    ts_e = pl.DataFrame(schema={'Date': pl.Date} | {i: pl.Float64 for i in site_list})
    ts_w = ts_l.with_columns(
        pl.col('Timestamp')
        .map_elements(clean_24h_datetime, return_dtype=pl.String)
        .str.slice(0, 10)
        .str.to_date('%Y-%m-%d')
        .alias('Date'),
        pl.col('Value').cast(pl.Float64).truediv(86400).name.keep(),
    ).pivot(on='Site', index='Date', values='Value')
    print('\nNote: The daily rate of take is in m^3/s!!!!\n')
    return pl.concat([ts_e, ts_w], how='diagonal').sort('Date').pipe(na_ts_insert)


def daily_Flo_AQ(
        site_list: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
) -> pl.DataFrame:
    """
    A wrapper of getting daily flow rate for multiple recorders (from Aquarius)

    Parameters
    ----------
    site_list : str | list[str]
        A list of plate numbers (for the flow recorders)
    date_start : int, optional, default=None
        Start date of the requested data. It follows '%Y%m%d' When specified.
        Otherwise, request the data from its very beginning.
    date_end : int, optional, default=None
        End date of the request data date. It follows '%Y%m%d' When specified.
        Otherwise, request the data till its end.
    raw_data : bool, optional, default=False
        Whether return the raw data (Timestamp in string) from Aquarius.

    Returns
    -------
    pl.DataFrame
        A DataFrame of daily flow rate
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    site_list = list(dict.fromkeys(site_list))
    ts_l = pl.concat([_DFlo_AQ(i, date_start, date_end) for i in site_list], how='vertical')
    if raw_data:
        print('\nNote: The (raw) daily flow is in m^3!!!!\n')
        return ts_l
    ts_e = pl.DataFrame(schema={'Date': pl.Date} | {i: pl.Float64 for i in site_list})
    ts_w = ts_l.with_columns(
        pl.col('Timestamp')
        .map_elements(clean_24h_datetime, return_dtype=pl.String)
        .str.slice(0, 10)
        .str.to_date('%Y-%m-%d')
        .alias('Date'),
        pl.col('Value').cast(pl.Float64).name.keep(),
    ).pivot(on='Site', index='Date', values='Value')
    print('\nNote: The daily flow rate is in m^3/s!!!!\n')
    ts = pl.concat([ts_e, ts_w], how='diagonal').sort('Date')
    sn = {i: get_site_name(i) for i in site_list}
    return ts.rename({k: v for k, v in sn.items() if v is not None}).pipe(na_ts_insert)


def _field_data_AQ_s(
        plate: str,
        /,
        parameters: 'str | list[str]' = None,
) -> 'pl.DataFrame | None':
    """A helper function to get the field visit data for a single site (plate)"""
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url = f'{end_point}/GetFieldVisitReadingsByLocation?LocationIdentifier={plate}'
    if parameters is None:
        parameters = []
    if isinstance(parameters, str):
        parameters = [parameters]
    for parameter in parameters:
        url += f'&Parameters={parameter}'
    r = get_AQ(url=url)
    if not (ld := json.loads(r.data.decode('utf-8')).get('FieldVisitReadings', None)):
        return None
    ts = pl.DataFrame(ld)
    ts.insert_column(0, pl.lit(plate).alias('Plate'))
    fmt = '%Y-%m-%dT%H:%M:%S'
    return (
        ts.with_columns(
            pl.col('Approval').struct.field('*'),
            pl.col('Value').struct.field('*'),
            pl.col('Time').map_elements(clean_24h_datetime, return_dtype=pl.String)
            .str.to_datetime(fmt)
            .name.keep(),
        )
        .drop(['Approval', 'Value'])
        .sort(['Parameter', 'Time'])
        .with_columns(pl.col('Time').dt.strftime(fmt).name.keep())
    )


def get_field_data_AQ(
        plate_list: 'str | list[str]',
        /,
        parameters: 'str | list[str]' = None,
) -> 'pl.DataFrame | None':
    """
    Get the field visit data (from Aquarius)

    Parameters
    ----------
    plate_list : str | list[str]
        The list of plate(s), single plate can be either a string or a list
    parameters : str | list[str], `default=None` for all possible field measurements as below
        - 'Air Temp'
        - 'Cond'
        - 'Dis Oxygen Sat'
        * 'Discharge'
        - 'Dissolved Oxygen'
        - 'GZF'
        - 'Gas Pressure'
        - 'Groundwater Level'
        - 'Hydraulic Radius'
        - 'Maximum Gauged Depth'
        - 'NO3 (Dis)'
        - 'O2 (Dis)'
        - 'PM 10'
        - 'Rainfall'
        - 'Rainfall Depth'
        - 'Sp Cond'
        * 'Stage'
        - 'Stage Change'
        - 'Stage Offset'
        - 'Tot Susp Sed'
        - 'Turbidity (Form Neph)'
        - 'Voltage'
        - 'Water Surface Slope'
        - 'Water Temp'
        - 'Water Velocity'
        - 'Wetted Perimeter'
        - 'pH'
        - 'pH Voltage'

    Returns
    -------
    pd.DataFrame | None
        Field visit data in a DataFrame

    Note
    ----
        https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2/swagger-ui/
    """
    if isinstance(plate_list, str):
        plate_list = [plate_list]
    ts = pl.DataFrame()
    for plate in pl.Series(plate_list).unique(maintain_order=True):
        if (tmp := _field_data_AQ_s(plate, parameters)) is None:
            print(f'No field visit data for - [{plate}] in the given parameter(s)')
            continue
        ts = pl.concat([ts, tmp], how='diagonal_relaxed')
    return None if ts.is_empty() else ts.rename({'Numeric': 'Value'})


get_stage_flow_AQ = partial(get_field_data_AQ, parameters=['Stage', 'Discharge'])


def _get_field_hydro_s(plate: str) -> pl.DataFrame:
    """A helper function for `get_field_hydro_AQ()`"""
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url = f'{end_point}/GetFieldVisitDataByLocation?LocationIdentifier={plate}'
    r = get_AQ(url)
    scm = {
        'Identifier': pl.String,
        'LocationIdentifier': pl.String,
        'MeasurementTime': pl.String,
        'GradeCode': pl.String,
        'Measurement': pl.String,
        'Unit': pl.String,
        'Numeric': pl.Float64,
    }
    empty_df = pl.DataFrame(schema=scm)
    if not (ld := json.loads(r.data.decode('utf-8')).get('FieldVisitData')):
        return empty_df
    cois = ['Identifier', 'LocationIdentifier', 'DischargeActivities']
    data = pl.DataFrame({coi: [i.get(coi) for i in ld] for coi in cois}, strict=False)
    if data.drop_nulls().is_empty():
        return empty_df
    t = (
        data.explode('DischargeActivities')
        .with_columns(
            pl.col('DischargeActivities').struct.field('DischargeSummary'),
            pl.col('DischargeActivities').struct.field('PointVelocityDischargeActivities'),
            pl.col('DischargeActivities').struct.field('AdcpDischargeActivities'),
        )
        .explode('PointVelocityDischargeActivities')
        .explode('AdcpDischargeActivities')
        .drop('DischargeActivities')
        .filter(
            pl.sum_horizontal(
                pl.col('DischargeSummary').is_not_null(),
                pl.col('PointVelocityDischargeActivities').is_not_null(),
                pl.col('AdcpDischargeActivities').is_not_null(),
            )
            .gt(0)
        )
        .with_columns(
            pl.coalesce(
                'AdcpDischargeActivities',
                'PointVelocityDischargeActivities',
            )
            .alias('AP')
        )
        .drop(
            [
                'AdcpDischargeActivities',
                'PointVelocityDischargeActivities',
            ]
        )
    )
    unpack_ds = t.select(pl.col('DischargeSummary').struct.field('*')).columns
    noi_ds = ['MeasurementTime', 'Discharge', 'MeanGageHeight', 'GradeCode']
    req_fld_ds = [i for i in noi_ds if i in unpack_ds]
    unpack_ap = t.select(pl.col('AP').struct.field('*')).columns
    noi_ap = ['Width', 'Area', 'VelocityAverage']
    req_fld_ap = [i for i in noi_ap if i in unpack_ap]
    idx_upvt = ['Identifier', 'LocationIdentifier', 'MeasurementTime', 'GradeCode']
    if 'GradeCode' not in req_fld_ds:
        idx_upvt.remove('GradeCode')
    fmt = '%Y-%m-%dT%H:%M:%S'
    tt = (
        t.with_columns(
            pl.col('DischargeSummary').struct.field(req_fld_ds),
            pl.col('AP').struct.field(req_fld_ap),
        )
        .drop(['DischargeSummary', 'AP'])
        .unpivot(
            index=idx_upvt,
            variable_name='Measurement',
            value_name='UV',
        )
        .with_columns(
            pl.col('MeasurementTime').str.head(19).str.to_datetime(fmt).name.keep(),
            pl.col('UV').struct.field('*'),
        )
        .drop('UV')
        .filter(pl.col('Numeric').ne(-1))
        .sort(['Measurement', 'MeasurementTime'])
        .with_columns(pl.col('MeasurementTime').dt.strftime(fmt).name.keep())
        .unique(maintain_order=True)
    )
    return pl.concat([empty_df, tt], how='diagonal_relaxed')


def get_field_hydro_AQ(site_list: 'str | list[str]') -> pl.DataFrame:
    """
    Get the field spot gauging data for multiple plates

    Parameters
    ----------
    site_list : str | list[str]
        A list of plate(s)

    Returns
    -------
    pl.DataFrame
        Get the field data for the following:
        - 'Discharge': 'DischargeSummary'
        - 'MeanGageHeight': 'DischargeSummary'
        - 'Width': 'AdcpDischargeActivities' -> 'PointVelocityDischargeActivities'
        - 'Area': 'AdcpDischargeActivities' -> 'PointVelocityDischargeActivities'
        - 'VelocityAverage': 'AdcpDischargeActivities' -> 'PointVelocityDischargeActivities'
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    scm = {
        'Identifier': pl.String,
        'LocationIdentifier': pl.String,
        'MeasurementTime': pl.String,
        'GradeCode': pl.String,
        'Measurement': pl.String,
        'Unit': pl.String,
        'Numeric': pl.Float64,
    }
    ts = pl.DataFrame(schema=scm)
    for plate in pl.Series(site_list).unique(maintain_order=True):
        print(f'Getting the field visit hydro data for - [{plate}]...')
        if (tmp := _get_field_hydro_s(plate)).is_empty():
            print(cp(f'\t-> No field hydro data for - [{plate}]!', fg=35, display=3))
            continue
        ts = pl.concat([ts, tmp], how='vertical')
    return ts


def get_url_uid(uid: str, date_start: int = None, date_end: int = None) -> str:
    """Makes the URL for getting the time series for a plate through UniqueId"""
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    q_dict = {'TimeSeriesUniqueId': uid}
    if date_start is not None:
        ds = datetime.datetime.strptime(f'{date_start}', '%Y%m%d')
        q_dict['QueryFrom'] = ds.strftime('%Y-%m-%dT00:00:00.0000000+12:00')
    if date_end is not None:
        de = datetime.datetime.strptime(f'{date_end}', '%Y%m%d') + datetime.timedelta(days=1)
        q_dict['QueryTo'] = de.strftime('%Y-%m-%dT00:00:00.0000000+12:00')
    q_str = parse.urlencode(q_dict)
    return f'{end_point}/GetTimeSeriesCorrectedData?{q_str}'


def get_ts(*args, **kwargs) -> pl.DataFrame:
    """Obtains the time series for a plate using the UniqueId"""
    r = get_AQ(get_url_uid(*args, **kwargs))
    d = json.loads(r.data.decode('utf-8'))
    if not d.get('Points'):
        err_msg = 'No time series is available\n'
        print(cp(f'\n{err_msg}', fg=35, display=1))
        return pl.DataFrame(
            schema={
                'Timestamp': pl.String,
                'Value': pl.Float64,
                'Unit': pl.String,
                'Identifier': pl.String,
            }
        )
    if r.reason != 'OK':
        err_msg = d.get('ResponseStatus').get('Errors')[0].get('Message')
        print(cp(f'\n{err_msg}', fg=35, display=1))
        return pl.DataFrame(
            schema={
                'Timestamp': pl.String,
                'Value': pl.Float64,
                'Unit': pl.String,
                'Identifier': pl.String,
            }
        )
    idfr = f"{d.get('Parameter')}.{d.get('Label')}@{d.get('LocationIdentifier')}"
    return (
        pl.DataFrame(d.get('Points'))
        .with_columns(
            pl.col('Timestamp').str.head(19).alias('Timestamp'),
            pl.col('Value').struct.unnest().alias('Value'),
            pl.lit(d.get('Unit')).alias('Unit'),
            pl.lit(idfr).alias('Identifier'),
        )
    )


class PlateType(type):
    def __repr__(self):
        return self.__name__


class Plate(metaclass=PlateType):
    """The class for a Plate object"""
    _fg_color = {
        'location': 33,
        'tag': 32,
        'ts_info': 36,
    }

    def __init__(self, plate: str = None):
        self.plate = plate

    def __str__(self) -> str:
        return f'\nAn object to obtain its metadata for plate "{self.plate}" from Aquarius!'

    def __repr__(self) -> str:
        return f'Plate({self.plate})'

    def r_plate(self, api: str) -> urllib3.response.BaseHTTPResponse:
        """Create a response for the specified plate"""
        end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
        u = f'{end_point}/{api}'
        return get_AQ(u, fields={'LocationIdentifier': self.plate})

    def exists(self) -> bool:
        """Check if the specified plate is valid (or exists)"""
        r = self.r_plate('GetLocationData')
        if r.reason != 'OK':
            d = json.loads(r.data.decode('utf-8'))
            print(cp(f"\n{d.get('ResponseStatus').get('Message')}", fg=35, display=1))
            return False
        return True

    @property
    def plate(self) -> str:
        return self._plate

    @plate.setter  # Set the limitations in here (optional)!
    def plate(self, value: str):
        self._plate = value

    def get_ts_info(self) -> 'pl.DataFrame | None':
        """Gets the frame having all related time series info for a plate"""
        r = self.r_plate('GetTimeSeriesDescriptionList')
        if self.exists():
            l = json.loads(r.data.decode('utf-8')).get('TimeSeriesDescriptions')
            d_col = {
                'Identifier': 'Identifier',
                'UniqueId': 'UniqueId',
                'Unit': 'Unit',
                'CorrectedStartTime': 'Start',
                'CorrectedEndTime': 'End',
            }
            return (
                pl.DataFrame(l)
                .select(d_col.keys())
                .rename(d_col)
                .with_columns(
                    pl.col('Start').str.head(19).name.keep(),
                    pl.col('End').str.head(19).name.keep(),
                )
                .sort('Identifier')
            )

    @property
    def ts_info(self) -> None:
        """Prints the frame having all related time series info for a plate"""
        with pl.Config() as cfg:
            cfg.set_tbl_rows(-1)
            cfg.set_tbl_hide_dataframe_shape(True)
            cfg.set_tbl_hide_column_data_types(True)
            print(cp(self.get_ts_info(), fg=self._fg_color.get('ts_info')))

    def get_location(self) -> 'pl.DataFrame | None':
        """Gets a frame having spatial details for a plate"""
        r = self.r_plate('GetLocationData')
        if self.exists():
            d = json.loads(r.data.decode('utf-8'))
            d_col = {
                'LocationName': 'Name',
                'Identifier': 'Plate',
                'LocationType': 'Type',
                'Longitude': 'E',
                'Latitude': 'N',
                'Srid': 'EPSG',
                'Elevation': 'Elevation',
                'ElevationUnits': 'ElevationUnit',
            }
            return (
                pl.DataFrame({v: d.get(k) for k, v in d_col.items()})
                .with_columns(
                    pl.when(pl.col('Elevation').eq(0))
                    .then(None)
                    .otherwise(pl.col('Elevation'))
                    .name.keep(),
                )
            )

    @property
    def name(self) -> str:
        """Get the site name for a plate"""
        if self.exists():
            return self.get_location().item(0, 'Name')

    @property
    def location(self) -> None:
        """Prints the spatial information for a plate"""
        with pl.Config() as cfg:
            cfg.set_tbl_rows(-1)
            cfg.set_tbl_hide_dataframe_shape(True)
            cfg.set_tbl_hide_column_data_types(True)
            print(cp(self.get_location(), fg=self._fg_color.get('location')))

    def get_tag(self) -> 'pl.DataFrame | None':
        """Gets the frame having all tag info for a plate"""
        r = self.r_plate('GetLocationData')
        d = json.loads(r.data.decode('utf-8'))
        if self.exists():
            return (
                pl.DataFrame({'Plate': d.get('Identifier'), 'Tags': d.get('Tags')})
                .with_columns(pl.col('Tags').struct.field(['Key', 'Value']))
                .drop('Tags')
            )

    @property
    def tag(self) -> None:
        """Prints the property of the tag info for a plate"""
        with pl.Config() as cfg:
            cfg.set_tbl_rows(-1)
            cfg.set_tbl_hide_dataframe_shape(True)
            cfg.set_tbl_hide_column_data_types(True)
            print(cp(self.get_tag(), fg=self._fg_color.get('tag')))

    def get_info(self) -> dict[str, pl.DataFrame]:
        """Gets a dictionary of three frames having all possible metadata for a plate"""
        return {
            'location': self.get_location(),
            'tag': self.get_tag(),
            'ts_info': self.get_ts_info(),
        }

    @property
    def info(self) -> None:
        """Prints the properties of all possible metadata for a plate"""
        with pl.Config() as cfg:
            cfg.set_tbl_rows(-1)
            cfg.set_tbl_hide_dataframe_shape(True)
            cfg.set_tbl_hide_column_data_types(True)
            for fg, (k, v) in zip(self._fg_color.values(), self.get_info().items()):
                print(
                    cp(cp(f'\n{k}:\n', fg=39, display=4), display=1)
                    + cp(f'\n{v}\n', fg=fg)
                )
