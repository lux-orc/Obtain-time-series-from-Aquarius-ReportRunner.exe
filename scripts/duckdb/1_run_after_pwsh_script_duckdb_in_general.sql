
/* Suggests use [uid] in the PowerShell script obtaining time series */

duckdb out/df_long.duckdb


-- Read the JSON files ('plate_info.json', 'plate_info.json') from <info> folder
set variable file_plate = 'info/plate_info.json';
-- set variable file_plate = (
--     'https://raw.githubusercontent.com/lux-orc/' ||
--     'TimeSeries_Aquarius/refs/heads/master/' ||
--     'info/plate_info.json'
-- );
create or replace table plates as
    with cte_pn as (
        select * as plate_name
        from read_json(getvariable('file_plate'))
        -- from 'info/plate_info.json'
    )
    select
        unnest(map_keys(plate_name)) as Plate,
        unnest(map_values(plate_name)) as Name
    from cte_pn
;
set variable file_param = 'info/param_info.json';
-- set variable file_param = (
--     'https://raw.githubusercontent.com/lux-orc/' ||
--     'TimeSeries_Aquarius/refs/heads/master/' ||
--     'info/param_info.json'
-- );
create or replace table params as
    with param_unit as (
        select * as param_unit
        from read_json(getvariable('file_param'))
        -- from 'info/param_info.json'
    )
    select
        unnest(map_keys(param_unit)) as Parameter,
        unnest(map_values(param_unit)) as Unit
    from param_unit
;


-- Create a long format frame before save
create or replace table ts_long as
    -- Read all CSV files in the <out> folder, recursively -> `tmp`
    with tmp as (
        select *
        from read_csv(
            'out/**/*.csv',
            skip = 11,
            types = {'TimeStamp': 'TIMESTAMP'},
            union_by_name = true,
            filename = true
        )
    ),
    -- Create a long-format frame (UNPIVOT)
    cte as (
        unpivot tmp
        on columns(* exclude (TimeStamp, filename))
        into
            name Parameter_Plate
            value Value
    ),
    -- Split column [Parameter_Plate] into columns [Parameter] and [Plate]
    tmp_long as (
        select
            TimeStamp,
            Value,
            parse_path(filename, 'system')[-2] as folder,
            split_part(Parameter_Plate, '@', -1) as Plate,
            list_aggregate(
                string_split(Parameter_Plate, '@')[:-2], 'string_agg', '@'
            ) as Parameter,
            parse_filename(filename, true, 'system') as uid
        from cte
    )
    -- Add the extra information - Unit, and Site names
    select
        t.TimeStamp, t.Value, pa.Unit, t.Parameter, pl.*, t.folder, t.uid
    from tmp_long t
    left join plates pl on t.Plate = pl.Plate
    left join params pa on t.Parameter = pa.Parameter
    -- Try not to use `order by` clause in CTE/subquery - use it in the main query instead!
    order by folder, Name, uid, TimeStamp
;


-- Show some summary about the merged data
copy (
    select
        any_value(Plate) as Plate,
        Name,
        folder,
        any_value(Unit) as Unit,
        min(TimeStamp) as Start,
        max(TimeStamp) as End,
        -- avg(Value).round(3) as Mean,
        -- stddev_samp(Value).round(3) as Std,  -- Use the sample standard deviation
        min(Value).round(3) as Min,
        arg_min(TimeStamp, Value) as Time_min,
        -- quantile_cont(Value, .25).round(3) as "25%",
        -- median(Value).round(3) as Median,
        -- quantile_cont(Value, .75).round(3) as "75%",
        max(Value).round(3) as Max,
        arg_max(TimeStamp, Value) as Time_max,
        uid,
    from ts_long
    group by folder, Name, uid
    order by folder, Name
) to 'out/data_range_db.tsv'
with (FORMAT CSV, DELIMITER '\t', HEADER);


-- show tables;


-- Use varchar for the TimeStamp column (this takes up more disc space)
create or replace table ts_long as
    select * replace (strftime(TimeStamp, '%Y-%m-%d %H:%M:%S') as TimeStamp)
    from ts_long;
-- Export the obtained data to a PARQUET file
copy ts_long to 'out/df_long.parquet';




-- EXPORT DATABASE 'out/long_duckdb' (FORMAT PARQUET);
-- IMPORT DATABASE 'out/long_duckdb';
-- show tables;

.exit
