
import datetime
import json
from functools import partial, reduce
from typing import Any, Callable
from urllib import parse

import numpy as np
import pandas as pd
import urllib3

# Some display settings for numpy Array, Pandas DataFrame
np.set_printoptions(precision=4, linewidth=94, suppress=True)
pd.set_option('display.max_columns', None)


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


def _ts_valid_pd(ts: Any, /) -> str:
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
        When `_ts_valid_pd(ts) is not None` is False.

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
    if (step := ts_step(ts)) in {-1, None}: return r
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
    date_new = (hts_c.index - pd.Timedelta(f'{3600 * (1+day_starts_at)}s')).date
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
    if (con := ts_step(ts)) is None: return None
    if isinstance(ts, pd.Series): ts = ts.to_frame()
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


def get_AQ(
        url: str,
        basic_auth: str = 'api-read:PR98U3SKOczINoPHo7WM',
        **kwargs
    ) -> urllib3.response.HTTPResponse:
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
        raise ValueError(cp("Provide a correct string value for 'Site'!\n", fg=35))
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
    ) -> pd.DataFrame:
    """Get the time series for a single site specified by those defined in `get_url_AQ`"""
    col_dtype = {'Timestamp': str, 'Value': float}
    empty_df = pd.DataFrame(columns=col_dtype.keys()).astype(col_dtype)
    if (url := get_url_AQ(measurement, site, date_start, date_end)) is None:
        print(cp(
            f'\n[{measurement}@{site}] -> No data! An empty column [{site}] added!\n',
            fg=34
        ))
        return empty_df
    r = get_AQ(url=url)
    if not (ld := json.loads(r.data.decode('utf-8')).get('Points', None)):
        print(cp(f'[{measurement}@{site}] -> No data over the chosen period!\n', fg=34))
        return empty_df
    return (
        pd.json_normalize(ld, sep='_')
        .rename(columns={'Value_Numeric': 'Value'})
        .astype(col_dtype)
    )


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
        datetime.datetime(*Ymd, H-1, M, S)
        + datetime.timedelta(hours=1)
    ).strftime('%Y-%m-%dT%H:%M:%S') if H > 23 else s19


def _HWU_AQ(
        site: str,
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False
    ) -> pd.DataFrame:
    """Get hourly rate for a single water meter (from Aquarius)"""
    ts_raw = get_ts_AQ('Flow.WMHourlyMean', site, date_start, date_end)
    if raw_data:
        return ts_raw
    return pd.DataFrame(
        {site: ts_raw['Value'].values / 1e3},
        index=ts_raw['Timestamp'].apply(clean_24h_datetime).pipe(pd.to_datetime),
    ).rename_axis(index='Time').pipe(na_ts_insert)


def _DWU_AQ(
        site: str,
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False
    ) -> pd.DataFrame:
    """Get daily rate for a single water meter (from Aquarius)"""
    ts_raw = get_ts_AQ('Abstraction Volume.WMDaily', site, date_start, date_end)
    if raw_data:
        return ts_raw
    return pd.DataFrame(
        {site: ts_raw['Value'].values / 86400},
        index=ts_raw['Timestamp'].apply(clean_24h_datetime).pipe(pd.to_datetime)
    ).rename_axis(index='Date').pipe(na_ts_insert)


def hourly_WU_AQ(
        site_list: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
    ) -> pd.DataFrame:
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
        Raw data (hourly volume in m^3) from Aquarius (extra info). Default is `False`

    Returns
    -------
    pd.DataFrame
        A DataFrame of hourly abstraction
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    site_list = list(dict.fromkeys(site_list))
    if raw_data:
        d = {site: _HWU_AQ(site, date_start, date_end, True) for site in site_list}
        for k, v in d.items():
            v.insert(0, 'Site', k)
        return pd.concat(d.values(), axis=0, join='outer', ignore_index=True)
    lst = [_HWU_AQ(site, date_start, date_end, False) for site in site_list]
    return reduce(lambda a, b: a.join(b, how='outer'), lst).pipe(na_ts_insert)


def daily_WU_AQ(
        site_list: 'str | list[str]',
        date_start: int = None,
        date_end: int = None,
        raw_data: bool = False,
    ) -> pd.DataFrame:
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
        Raw data (daily volume in m^3) from Aquarius (extra info). Default is `False`

    Returns
    -------
    pd.DataFrame
        A DataFrame of daily abstraction
    """
    if isinstance(site_list, str):
        site_list = [site_list]
    site_list = list(dict.fromkeys(site_list))
    if raw_data:
        d = {site: _DWU_AQ(site, date_start, date_end, True) for site in site_list}
        for k, v in d.items():
            v.insert(0, 'Site', k)
        return pd.concat(d.values(), axis=0, join='outer', ignore_index=True)
    lst = [_DWU_AQ(site, date_start, date_end, False) for site in site_list]
    return reduce(lambda a, b: a.join(b, how='outer'), lst).pipe(na_ts_insert)


def _field_data_AQ_s(
        plate: str,
        /,
        parameters: 'str | list[str]' = None,
) -> 'pd.DataFrame | None':
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
    ts = pd.json_normalize(ld)
    ts.insert(0, 'Plate', plate)
    fmt = '%Y-%m-%dT%H:%M:%S'
    ts['Time'] = pd.to_datetime(ts['Time'].map(clean_24h_datetime), format=fmt)
    ts = ts.sort_values(['Parameter', 'Time'])
    ts['Time'] = ts['Time'].dt.strftime(fmt)
    return ts.reset_index(drop=True)


def get_field_data_AQ(
        plate_list: 'str | list[str]',
        /,
        parameters: 'str | list[str]' = None,
) -> 'pd.DataFrame | None':
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
    ts = pd.DataFrame()
    for plate in pd.Series(plate_list).unique():
        if (tmp := _field_data_AQ_s(plate, parameters)) is None:
            print(f'No field visit data for - [{plate}] in the given parameter(s)')
            continue
        ts = pd.concat([ts, tmp], axis=0)
    return (
        None if ts.empty else
        ts.rename(columns={'Value.Unit': 'Unit', 'Value.Numeric': 'Value'})
        .reset_index(drop=True)
    )


get_stage_flow_AQ = partial(get_field_data_AQ, parameters=['Stage', 'Discharge'])


def _get_field_hydro_s(plate: str) -> pd.DataFrame:
    """A helper function for `get_field_hydro_AQ()`"""
    end_point = 'https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2'
    url = f'{end_point}/GetFieldVisitDataByLocation?LocationIdentifier={plate}'
    r = get_AQ(url)
    scm = {
        'Identifier': str,
        'LocationIdentifier': str,
        'MeasurementTime': str,
        'GradeCode': str,
        'Measurement': str,
        'Unit': str,
        'Numeric': float,
    }
    empty_df = pd.DataFrame(columns=scm.keys()).astype(scm)
    if not (ld := json.loads(r.data.decode('utf-8')).get('FieldVisitData')):
        return empty_df
    cois = ['Identifier', 'LocationIdentifier', 'DischargeActivities']
    data = pd.DataFrame({coi: [i.get(coi) for i in ld] for coi in cois})
    if data.dropna().empty:
        return empty_df
    tmp = data.explode('DischargeActivities')
    coi = [
        'DischargeSummary',
        'PointVelocityDischargeActivities',
        'AdcpDischargeActivities',
    ]
    for c in coi:
        tmp[c] = tmp['DischargeActivities'].map(
            lambda i: i.get(c) if isinstance(i, dict) else None
        )
    tmp = (
        tmp.explode('PointVelocityDischargeActivities')
        .explode('AdcpDischargeActivities')
        .drop(columns='DischargeActivities')
        .dropna(
            subset=[
                'DischargeSummary',
                'PointVelocityDischargeActivities',
                'AdcpDischargeActivities',
            ],
            how='all'
        )
    )
    tmp['AP'] = (
        tmp['AdcpDischargeActivities']
        .combine_first(tmp['PointVelocityDischargeActivities'])
    )
    tmp = tmp.drop(columns=['AdcpDischargeActivities', 'PointVelocityDischargeActivities'])
    for i in ['MeasurementTime', 'Discharge', 'MeanGageHeight', 'GradeCode']:
        tmp[i] = tmp['DischargeSummary'].map(
            lambda d: d.get(i) if isinstance(d, dict) else None
        )
    for i in ['Width', 'Area', 'VelocityAverage']:
        tmp[i] = tmp['AP'].map(lambda d: d.get(i) if isinstance(d, dict) else None)
    tmp = tmp.drop(columns=['DischargeSummary', 'AP'])
    fmt = '%Y-%m-%dT%H:%M:%S'
    t = tmp.melt(
        id_vars=[
            'Identifier',
            'LocationIdentifier',
            'MeasurementTime',
            'GradeCode',
        ],
        var_name='Measurement',
        value_name='UV',
    )
    t['Unit'] = t['UV'].map(lambda i: i.get('Unit') if isinstance(i, dict) else None)
    t['Numeric'] = t['UV'].map(lambda i: i.get('Numeric') if isinstance(i, dict) else None)
    t['MeasurementTime'] = pd.to_datetime(t['MeasurementTime'].str[:19], format=fmt)
    t.loc[t['Numeric'] == -1, 'Numeric'] = np.nan
    tt = (
        t.drop(columns='UV')
        .dropna(subset=['Numeric'])
        .drop_duplicates()
        .sort_values(['Measurement', 'MeasurementTime'])
        .reset_index(drop=True)
    )
    tt['MeasurementTime'] = tt['MeasurementTime'].dt.strftime(fmt)
    tt['GradeCode'] = tt['GradeCode'].map(lambda x: None if pd.isna(x) else str(int(x)))
    return tt.astype(scm)


def get_field_hydro_AQ(site_list: 'str | list[str]') -> pd.DataFrame:
    """
    Get the field spot gauging data for multiple plates

    Parameters
    ----------
    site_list : str | list[str]
        A list of plate(s)

    Returns
    -------
    pd.DataFrame
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
        'Identifier': str,
        'LocationIdentifier': str,
        'MeasurementTime': str,
        'GradeCode': str,
        'Measurement': str,
        'Unit': str,
        'Numeric': float,
    }
    ts = pd.DataFrame(columns=scm.keys()).astype(scm)
    for plate in pd.Series(site_list).unique():
        print(f'Getting the field visit hydro data for - [{plate}]...')
        if (tmp := _get_field_hydro_s(plate)).empty:
            print(cp(f'\t-> No field hydro data for - [{plate}]!', fg=35, display=3))
            continue
        ts = pd.concat([ts, tmp], axis=0)
    ts['GradeCode'] = ts['GradeCode'].map(lambda s: None if s == 'None' else s)
    return ts.reset_index(drop=True)
