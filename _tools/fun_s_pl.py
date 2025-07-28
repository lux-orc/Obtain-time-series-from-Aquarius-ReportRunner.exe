# -*- coding: utf-8 -*-
import datetime
from typing import Any, Callable

import numpy as np
import pandas as pd
import polars as pl
import polars.selectors as cs

# Some display settings for numpy Array, Pandas and Polars DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)
pl.Config.set_fmt_str_lengths(80)
pl.Config.set_tbl_cols(-1)


def cp(s: Any = '', /, display: int = 0, fg: int = 39, bg: int = 48) -> str:
    """Return the string for color print in the (IPython) console"""
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
        time_no_hms = all(
            [
                ts[col_dt].dt.hour().eq(0).all(),
                ts[col_dt].dt.minute().eq(0).all(),
                ts[col_dt].dt.second().eq(0).all(),
            ]
        )
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
    col_dt = ts.select(cs.date() | cs.datetime()).columns[0]
    r = ts.lazy().fill_nan(None).filter(~pl.all_horizontal(cs.numeric().is_null()))
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
        if pl.Date.is_(ts[col_dt].dtype)
        else pl.datetime_range(s, e, f'{step}s')
    )
    dt: pl.LazyFrame = pl.LazyFrame().with_columns(dt_col.alias(col_dt))
    return dt.join(r, on=col_dt, how='left').sort(col_dt).collect()


def hourly_2_daily(
    hts: pl.DataFrame,
    day_starts_at: int = 0,
    agg: Callable = pl.mean,
    prop: float = 1.0,
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
        .group_by('Site', maintain_order=True)
        .agg(
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
