
import json
import time
from pathlib import Path

import duckdb
import pandas as pd
import _tools.fun_s as fpd

time_start = time.perf_counter()

pd.set_option('display.max_columns', None)  # Show all columns


# Set up the path of the project
path = Path.cwd()
path_out = path / 'out'
path_csv = path_out / 'csv'
path_info = path / 'info'

# Check if folder <out/csv> exists, raise otherwise
if not path_csv.exists():
    raise FileNotFoundError(
        fpd.cp(f"Folder <{path_csv.relative_to(path)}> doesn't exist!", fg=35)
    )


# Read the reference flow sites (LocationIdentifier/Site)
with open(path_info / 'plate_info.json', 'r') as fi:
    plate_dict = json.load(fi)

# Read the parameters and units
param_dict = json.loads((path_info / 'param_info.json').read_text())


# Detect the folders in `path_csv` folder
path_folders = [i for i in path_csv.iterdir() if i.is_dir()]
# Get a quick idea of how many of all the csv data file(s) are stored in `path_csv` folder
csv_files = [i for i in path_csv.rglob('*.csv') if i.is_file()]


# Make a frame to store the long-format frame fro each folder inside the csv folder
ts_l = pd.DataFrame()

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
            + fpd.cp(f'<{path_folder.relative_to(path)}>\n', fg=33)
        )
        continue

    # Make the DataFrame for each `folder_name`
    ts = pd.DataFrame()
    for csv_path in csv_paths:
        tmp = pd.read_csv(csv_path, header=11)
        *param_part, plate = tmp.columns[-1].split('@')
        param = '@'.join(param_part)
        desc_list = pd.read_csv(csv_path, nrows=1, skiprows=6).iat[0, 0].split(': ')
        desc = desc_list[-1]
        uid_hyphen, lab = (
            desc_list[0]
            .replace('# ', '')
            .replace(f'@{plate}', '')
            .replace(f'{param}.', '')
            .split(' ', maxsplit=1)
        )
        # To make some column names the same as those from 'aquarius.orc.govt.nz/AQUARIUS'
        tmp = tmp.rename(columns={tmp.columns[-1]: 'Value'}).dropna().assign(
            Unit=param_dict.get(param),
            ts_id=f'{param}.{lab}@{plate}',
            Parameter=param,
            Label=lab,
            Plate=plate,
            Name=plate_dict.get(plate),
            uid=uid_hyphen.replace('-', ''),
            CSV=f'{csv_path.name}',
            Description=desc,
        )
        ts = pd.concat([ts, tmp], axis=0, sort=False, ignore_index=True)

    # Store the time series from each folder in csv filder as an item in a dictionary
    ts_l = pd.concat(
        [ts_l, ts.assign(folder=folder_name)],
        axis=0,
        sort=False,
        ignore_index=True,
    )

    # Save the data as a parquet (for data sharing purpose) from this folder
    parquet_2_save = path_out / f'{folder_name}.parquet'
    ts.to_parquet(parquet_2_save)
    print(
        '\nThe CSV files in folder '
        + fpd.cp(f'<{path_folder.relative_to(path)}>', fg=33)
        + ' exported as '
        + fpd.cp(f'{parquet_2_save.relative_to(path)}', fg=36),
    )

    # To convert the 'tidy' data to the wide Frame:
    # - Ensure that the Site/Plate is unique (for the wide format conversion)
    if ts['Plate'].unique().size < len(csv_paths):
        nloc_df = ts[['Plate', 'CSV']].drop_duplicates()
        nloc_df['C'] = nloc_df.groupby('Plate').transform(pd.Series.count)
        loc_dup = nloc_df.query('C > 1')['CSV'].tolist()
        print(
            fpd.cp(
                '\tWide format is ignored due to '
                f'the duplicated site names from files:\t{sorted(loc_dup)}\n',
                fg=34,
            )
        )
        continue

    # - Ensure that 'Unit' and 'Parameter' are uniform (for each folder having the data)
    if ts[['Unit', 'Parameter']].drop_duplicates().shape[0] > 1:
        print(
            fpd.cp(
                "\tWide format is ignored as data's `Unit` & `Parameter` from "
                f'<{path_folder.relative_to(path)}> are NOT uniform\n',
                fg=34,
            )
        )
        continue

    # - Ensure the time series having regular time step (<= 1 day)
    udt = pd.to_datetime(ts['TimeStamp'], format='%Y-%m-%d %H:%M:%S').unique()
    udt_df = pd.DataFrame({'VV': 0}, index=sorted(udt))
    step = fpd.ts_step(udt_df)
    if step == -1 or step > 86400:
        print(
            fpd.cp(
                '\tWide format is ignored due to:\n'
                '\t\t* either an irregular time step, or\n'
                '\t\t* a time step > 1 day\n',
                fg=34,
            )
        )
        continue

    # When all criteria being met, make a wide Frame
    name_idx = 'Date' if step == 86400 else 'Time'
    w = (
        ts.pivot(columns='Name', values='Value', index='TimeStamp')
        .loc[:, ts['Name'].unique()]
        .reset_index()
    )
    w['TimeStamp'] = pd.to_datetime(w['TimeStamp'], format='%Y-%m-%d %H:%M:%S')
    ts_w = (
        w.rename(columns={'TimeStamp': name_idx})
        .sort_values(name_idx)
        .set_index(name_idx)
        .pipe(fpd.na_ts_insert)
    )

    # Save the wide format
    parquet_2_save_wide = path_out / f'{folder_name}_wide.parquet'
    ts_w.reset_index().astype({name_idx: str}).to_parquet(parquet_2_save_wide)
    print(
        '\t'
        + fpd.cp(f'{parquet_2_save.relative_to(path)}', fg=36)
        + ' -> '
        + fpd.cp(f'{parquet_2_save_wide.relative_to(path)}', fg=35),
        end='\n\n',
    )


# Make a spreadsheet output for data chaecking purposes
tsv_2_save = path_out / 'data_range_pd.tsv'
ts_l['TimeStamp'] = pd.to_datetime(ts_l['TimeStamp'], format='%Y-%m-%d %H:%M:%S')
q_str = """
    select
        any_value(Plate) as Plate,
        Name,
        folder,
        ts_id,
        any_value(Description) as Description,
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
        CSV,
    from ts_l
    group by folder, Name, ts_id, CSV
    order by folder, Name, ts_id
"""
duckdb.sql(q_str).write_csv(file_name=f'{tsv_2_save}', sep='\t')


# Print out something showing it runs properly
print(fpd.cp(f'Time elapsed:\t{(time.perf_counter() - time_start):.3f} seconds.', fg=34))
