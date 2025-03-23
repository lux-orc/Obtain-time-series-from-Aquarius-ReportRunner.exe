
"""
Notes:
    There is no procedure to validate the read data!
    To validate the data, special filters are needed:
        eg, rainfall/flow data cannot be negative, etc.,
"""

import json
import time
from pathlib import Path

import duckdb
import polars as pl
import _tools.fun_s_pl as fpl

time_start = time.perf_counter()

pl.Config.set_tbl_cols(-1)  # Show all columns
# pl.Config.set_tbl_rows(50)  # Show number of rows


# Set up the path of the project
path = Path.cwd()
path_out = path / 'out'
path_csv = path_out / 'csv'
path_info = path / 'info'

# Check if folder <out/csv> exists, raise otherwise
if not path_csv.exists():
    raise FileNotFoundError(
        fpl.cp(f"Folder <{path_csv.relative_to(path)}> doesn't exist!", fg=35)
    )


# Read the reference (LocationIdentifier/Site)
plate_dict = json.loads((path_info / 'plate_info.json').read_text())

# Read the parameters and units
with open(path_info / 'param_info.json', 'r') as fi:
    param_dict = json.load(fi)


# Detect the folders in `path_csv` folder
path_folders = [i for i in path_csv.iterdir() if i.is_dir()]
# Get a quick idea of how many of all the csv data file(s) are stored in `path_csv` folder
csv_files = [i for i in path_csv.rglob('*.csv') if i.is_file()]


# Make a frame to store the long-format frame fro each folder inside the csv folder
ts_l = pl.DataFrame()

# For each folder, read the csv data files
for path_folder in path_folders:

    # Get the list of CSV files in full path
    csv_paths = [i for i in path_folder.iterdir() if i.is_file()]
    csv_names = [i.name for i in csv_paths]
    folder_name = path_folder.stem

    # Check if the respective folder having CSV data file(s)
    if not csv_names:
        print(
            '\nNo CSV files in folder '
            + fpl.cp(f'<{path_folder.relative_to(path)}>\n', fg=33)
        )
        continue

    # Make the DataFrame for each `folder_name`
    ts = pl.DataFrame()
    for csv_path in csv_paths:
        tmp = pl.read_csv(csv_path, skip_rows=11, schema_overrides=[pl.String, pl.Float64])
        *param_part, plate = tmp.columns[-1].split('@')
        param = '@'.join(param_part)
        desc_list = (
            pl.read_csv(
                csv_path,
                n_rows=1,
                skip_rows=6,
                truncate_ragged_lines=True,
            )
            .item(0, 0)
            .split(': ')
        )
        desc = desc_list[-1]
        uid_hyphen, lab = (
            desc_list[0]
            .replace('# ', '')
            .replace(f'@{plate}', '')
            .replace(f'{param}.', '')
            .split(' ', maxsplit=1)
        )
        # To make some column names the same as those from 'aquarius.orc.govt.nz/AQUARIUS'
        tmp = tmp.rename({tmp.columns[-1]: 'Value'}).drop_nulls().with_columns(
            pl.lit(param_dict.get(param)).alias('Unit'),
            pl.lit(f'{param}.{lab}@{plate}').alias('ts_id'),
            pl.lit(param).alias('Parameter'),
            pl.lit(lab).alias('Label'),
            pl.lit(plate).alias('Location'),
            pl.lit(plate_dict.get(plate)).alias('Site'),
            pl.lit(uid_hyphen.replace('-', '')).alias('uid'),
            pl.lit(f'{csv_path.name}').alias('CSV'),
            pl.lit(desc).alias('Description'),
        )
        ts = pl.concat([ts, tmp], how='diagonal')

    # Store the time series from each folder inside the csv filder
    ts_l = pl.concat(
        [
            ts_l,
            ts.with_columns(
                pl.col.TimeStamp.str.to_datetime('%Y-%m-%d %H:%M:%S').name.keep(),
                pl.lit(folder_name).alias('folder'),
            )
        ],
        how='vertical',
    )


    # Save the data as a parquet (for data sharing purpose) from this folder
    parquet_2_save = path_out / f'{folder_name}.parquet'
    ts.write_parquet(
        parquet_2_save,
        # compression='zstd',
        # compression_level=22,
    )
    print(
        '\nThe CSV files in folder '
        + fpl.cp(f'<{path_folder.relative_to(path)}>', fg=33)
        + ' exported as '
        + fpl.cp(f'{parquet_2_save.relative_to(path)}', fg=36),
    )

    # To convert the 'tidy' data to the wide Frame:
    # - Ensure that the Site/Plate is unique (for the wide format conversion)
    if ts['Location'].n_unique() < len(csv_paths):
        loc_dup = (
            ts.select(pl.col('Location', 'CSV'))
            .unique()
            .filter(pl.col('CSV').n_unique().over('Location').gt(1))
            .get_column('CSV')
            .to_list()
        )
        print(
            fpl.cp(
                '\tWide format is ignored due to '
                f'the duplicated site names from files:\t{sorted(loc_dup)}\n',
                fg=34,
            )
        )
        continue

    # - Ensure that 'Unit' and 'Parameter' are uniform (for each folder having the data)
    if ts.select(['Unit', 'Parameter']).unique().height > 1:
        print(
            fpl.cp(
                "\tWide format is ignored as data's `Unit` & `Parameter` from "
                f'<{path_folder.relative_to(path)}> are NOT uniform\n',
                fg=34,
            )
        )
        continue

    # - Ensure the time series having regular time step (<= 1 day)
    udt_df = pl.DataFrame(
        {
            'Time': ts['TimeStamp'].unique().str.to_datetime('%Y-%m-%d %H:%M:%S').sort(),
            'VV': 0,
        }
    )
    step = fpl.ts_step(udt_df)
    if step == -1 or step > 86400:
        print(
            fpl.cp(
                '\tWide format is ignored due to:\n'
                '\t\t* either an irregular time step, or\n'
                '\t\t* a time step > 1 day\n',
                fg=34,
            )
        )
        continue

    # When all criteria being met, make a wide Frame
    w = (
        ts.pivot(on='Site', index='TimeStamp', values='Value')
        .with_columns(pl.col('TimeStamp').str.to_datetime('%Y-%m-%d %H:%M:%S'))
        .sort(by='TimeStamp')
    )
    daily_dict = {
        True: ['Date', '%Y-%m-%d', pl.Date],
        False: ['Time', '%Y-%m-%d %H:%M:%S', pl.Datetime],
    }
    col_dt, fmt, fdt = daily_dict.get(fpl.is_ts_daily(udt_df))
    ts_w = (
        w.rename({'TimeStamp': col_dt})
        .cast({pl.Datetime: fdt})
        .pipe(fpl.na_ts_insert)
    )

    # Save the wide format as a parquet file
    parquet_2_save_wide = path_out / f'{folder_name}_wide.parquet'
    ts_w.with_columns(
        pl.col(col_dt).dt.strftime(fmt)
    ).write_parquet(
        parquet_2_save_wide,
        # compression='zstd',
        # compression_level=22,
    )
    print(
        '\t'
        + fpl.cp(f'{parquet_2_save.relative_to(path)}', fg=36)
        + ' -> '
        + fpl.cp(f'{parquet_2_save_wide.relative_to(path)}', fg=35),
        end='\n\n',
    )


# Make a spreadsheet output for data chaecking purposes
tsv_2_save = path_out / 'data_range_pl.tsv'
q_str = """
    select
        any_value(Location) as Location,
        Site,
        folder,
        any_value(Unit) as Unit,
        min(TimeStamp) as Start,
        max(TimeStamp) as End,
        -- avg(Value).round(3) as Mean,
        -- stddev_samp(Value).round(3) as Std,
        min(Value).round(3) as Min,
        arg_min(TimeStamp, Value) as Time_min,
        -- quantile_cont(Value, .25).round(3) as "25%",
        -- median(Value).round(3) as Median,
        -- quantile_cont(Value, .75).round(3) as "75%",
        max(Value).round(3) as Max,
        arg_max(TimeStamp, Value) as Time_max,
    from ts_l
    group by folder, Site
    order by folder, Site
"""
duckdb.sql(q_str).write_csv(file_name=f'{tsv_2_save}', sep='\t')


# Print out something showing it runs properly
print(fpl.cp(f'Time elapsed:\t{(time.perf_counter() - time_start):.3f} seconds.', fg=34))
