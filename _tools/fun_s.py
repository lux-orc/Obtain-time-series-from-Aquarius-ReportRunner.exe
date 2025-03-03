
import datetime
from typing import Any, Callable

import numpy as np
import pandas as pd

# Some display settings for numpy Array, Pandas DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)


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


def ts_step(
        ts: 'pd.DataFrame | pd.Series',
        minimum_time_step_in_second: int = 60
) -> 'int | None':
    """
    Identify the temporal resolution (in seconds) for a time series

    Parameters
    ----------
    ts : pd.DataFrame
        A Pandas DataFrame indexed by time/date.
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
    if err_str := _ts_valid_pd(ts):
        raise TypeError(cp(err_str, fg=35))
    x = ts.dropna(axis=0, how='all')
    if x.shape[0] in (0, 1):
        return None
    diff_in_second = (pd.Series(x.index).diff() / np.timedelta64(1, 's')).values[1:]
    step_minimum = diff_in_second[diff_in_second >= minimum_time_step_in_second].min()
    return int(step_minimum) if (diff_in_second % step_minimum == 0).all() else -1


def na_ts_insert(ts: 'pd.DataFrame | pd.Series') -> pd.DataFrame:
    """
    Pad NaN value into a Timestamp-indexed DataFrame or Series

    Parameters
    ----------
    ts : pd.DataFrame | pd.Series
        A Pandas DataFrame or pd.Series indexed by time/date.

    Returns
    -------
    pd.DataFrame
        The NaN-padded Timestamp-indexed Series/DataFrame.

    Notes
    -----
        * As for irregular time series, The empty-row-removed DataFrame returned.
        * The attributes in `ts.attrs` is maintained after using it.
    """
    r = pd.DataFrame(ts).dropna(axis=0, how='all')
    if (step := ts_step(ts)) in {-1, None}:
        return r
    r = r.asfreq(freq=f'{step}s')
    r.index.freq = None
    r.attrs = ts.attrs
    return r


def hourly_2_daily(
        hts: 'pd.DataFrame | pd.Series',
        day_starts_at: int = 0,
        agg: Callable = pd.Series.mean,
        prop: float = 1.
) -> pd.DataFrame:
    """
    Aggregate the hourly time series to daily time series using customised function

    Parameters
    ----------
    hts : pd.DataFrame | pd.Series
        An hourly time series (for a single site)
    day_starts_at : int, optional, default=0
        What time (hour) a day starts - 0 o'clock by default.
        e.g., 9 means the output of daily time series by 9 o'clock!
    agg : Callable, optional, default=pd.Series.mean
        Customised aggregation function - mean by default (`pd.Series.mean`)
    prop : float, optional, default=1
        The ratio of the available data (within a day range)

    Returns
    -------
    pd.DataFrame
        A daily time series (pd.DataFrame) with an extra column of site name

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
    hts_c = na_ts_insert(hts).dropna()
    site = hts_c.columns[0]
    date_new = (hts_c.index - pd.Timedelta(f'{3600 * (1 + day_starts_at)}s')).date
    u = pd.DataFrame({'Date': date_new, 'Value': hts_c.squeeze().values}).set_index('Date')
    u['Prop'] = u.groupby('Date', sort=False)['Value'].transform('size') / 24
    return (
        u.query('Prop >= @prop')
        .groupby('Date', sort=False)
        .agg(Agg=('Value', agg))
        .rename(columns={'Agg': f'Agg_{agg.__name__}'})
        .pipe(na_ts_insert)
        .assign(Site=site)
    )


def ts_info(ts: 'pd.DataFrame | pd.Series') -> pd.DataFrame:
    """
    Obtain the Timestamp-indexed time series (ts) data availability

    Parameters
    ----------
    ts : pd.DataFrame
        A Pandas DataFrame indexed by time/date.

    Returns
    -------
    pd.DataFrame
        Info on ['Site', 'Start', 'End', 'Length_yr', 'Completion_%'].
        As for time series of irregular time step, 'Completion_%' column is ignored.
    """
    if (con := ts_step(ts)) is None:
        return None
    if isinstance(ts, pd.Series):
        ts = ts.to_frame()
    col_name = pd.Index(ts.columns.tolist(), dtype=str, name='Site')
    col_name_ = [f'{i}_' for i in col_name]
    empty_df = pd.DataFrame(index=pd.Index(col_name_, dtype=str, name='Site'))
    ts_w = ts.reset_index()
    ts_w.columns = ['Time'] + col_name_
    ts_l = ts_w.melt(id_vars='Time', var_name='Site', value_name='V').dropna()
    info = ts_l.groupby('Site', sort=False).agg(
        Start=('Time', 'min'),
        End=('Time', 'max'),
        n=('V', pd.Series.count),
    )
    d_yr = 365.2422
    info['Length_yr'] = (info['End'] - info['Start']) / pd.Timedelta(f'{d_yr}D')
    info = empty_df.join(info, how='left').set_index(col_name).reset_index()
    if con == -1: return info.drop('n', axis=1)
    step_day = con / (3600 * 24)
    info = info.assign(
        N=info['Length_yr'] * d_yr + step_day,
        Length_yr=info['Length_yr'] + step_day / d_yr,
    )
    info['Completion_%'] = info['n'] * step_day / info['N'] * 100
    return info.drop(columns=['n', 'N'])
