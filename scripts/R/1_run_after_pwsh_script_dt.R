
rm(list = ls(all.names = TRUE))

library(data.table)  # Fast operations on large data frames

# https://rdatatable.gitlab.io/data.table/articles/datatable-faq.html

source("_tools/fun_s.R")

time_start <- Sys.time()  # Start the timer

# Set up the path of the project folder
path <- getwd()
path_out <- file.path(path, "out")
path_csv <- file.path(path_out, "csv")

# Check if folder <out/csv> exists, raise otherwise
if (!dir.exists(path_csv))
  stop(paste0("\n\tFolder <", path_relative(path_csv, path), "> doesn't exist!"))

# Load the reference list between the plate numbers and the site names
path_info <- file.path(path, "info")
plate_info <- fread(file.path(path_info, "plate_info.csv"), key = "Plate")
param_info <- fread(file.path(path_info, "param_info.csv"), key = "Param")


# Detect the folders in '/out/csv' folder
path_folders <- list.dirs(path_csv, recursive = FALSE, full.names = TRUE)

# Make a frame to store the long-format frame for each folder inside the csv folder
ts_l <- data.table()

# For each folder, read the csv data files
for (path_folder in path_folders) {
  csv_paths <- list.files(path_folder, recursive = FALSE, full.names = TRUE)
  csv_names <- list.files(path_folder, recursive = FALSE, full.names = FALSE)
  folder_name <- sub(pattern = paste0(path_csv, "/"), replacement = "", x = path_folder)
  pr <- path_relative(path_folder, path)
  if (!length(csv_names)) {
    message("\nNo CSV files in folder <", pr, ">\n", sep = "")
    next
  }
  ts_df <- NULL
  for (csv_path in csv_paths) {
    desc_df <- fread(csv_path, nrows = 1, sep = ":", sep2 = "#", skip = 6)
    desc <- desc_df[[1, 2]]
    tmp <- sub(x = desc_df[[1, 1]], replacement = "", pattern = "# ", fixed = TRUE)
    ts_i <- fread(csv_path, skip = 11, colClasses = list(character = "TimeStamp"))
    tmp_1 <- tstrsplit(names(ts_i)[2], "@", fixed = TRUE) |> unlist()
    plate <- rev(tmp_1)[1]
    param <- paste(tmp_1[tmp_1 != plate], collapse = "@")
    tmp_2 <-
      sub(pattern = paste0("@", plate), replacement = "", x = tmp) |>
      sub(pattern = paste0(param, "."), replacement = "", x = _) |>
      tstrsplit(" ", fixed = TRUE) |>
      unlist()
    lab <- paste(tmp_2[-1], collapse = " ")
    ts_i[, let(
      Unit = param_info[param, Unit],
      ts_id = paste0(param, ".", lab, "@", plate),
      Parameter = param,
      Label = lab,
      Plate = plate,
      Name = plate_info[plate, Name],
      uid = tmp_2[1] |> gsub(replacement = "", pattern = "-", fixed = TRUE),
      CSV = tstrsplit(csv_path, "/", fixed = TRUE) |> rev() |> _[[1]],
      Description = desc
    )]
    setnames(ts_i, old = names(ts_i)[2], new = "Value")
    ts_df <- rbindlist(list(ts_df, na.omit(ts_i, cols = "Value")))
  }
  
  # Store the time series from each folder inside the csv folder
  ts_l <- rbind(ts_l, data.table(ts_df, folder = folder_name))
  
  # Save the data from this folder
  parquet_2_save <- file.path(path_out, paste0(folder_name, ".parquet"))
  arrow::write_parquet(as.data.frame(ts_df), parquet_2_save)
  msg <- paste0(
    "\nThe data from folder <", pr, "> has been saved as ",
    '"', path_relative(parquet_2_save, path), '"'
  )
  message(msg)
  # Convert to wide format for the time series of a regular time step when:
  if (ts_df[, length(unique(Plate))] < length(csv_paths)) {
    loc_dup <-
      ts_df[, .(Plate = unique(Plate)), CSV][
        , .(CSV, C = .N), Plate][
          C > 1, sort(CSV)]
    cat(cp(
      paste0(
        "Wide format is ignored due to the duplicated site names from files: [",
        paste(loc_dup, collapse = ", "),
        "]"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  if (unique(ts_df[, .(Unit, Parameter)])[, .N] > 1) {
    cat(cp(
      paste0(
        "Wide format is ignored as data's `Unit` & `Parameter` from <",
        pr,
        "> are NOT uniform!"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  udt_df <- ts_df[, .(
    Time = as.POSIXct(unique(TimeStamp), format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-12"),
    VV = 0
  )]
  step_sec <- ts_step(udt_df)
  if (step_sec == -1 || step_sec > 86400) {
    cat(cp(
      paste(
        "Wide format is ignored as the time series is",
        "either in irregular time step or its time step > a day!"
      ),
      fg = 35
    ), "\n", sep = "")
    next
  }
  w <- dcast(ts_df, formula = TimeStamp ~ Name, value.var = "Value")
  w[, TimeStamp := as.POSIXct(TimeStamp, format = "%Y-%m-%d %H:%M:%S", tz = "Etc/GMT-12")]
  ts_w <- na_ts_insert(w)[, c("TimeStamp", unique(ts_df$Name)), with = FALSE]
  if (step_sec == 86400) {
    setnames(ts_w, old = "TimeStamp", new = "Date")
    ts_w[, Date := substr(as.character(Date), 1, 10)]
  } else {
    setnames(ts_w, old = "TimeStamp", new = "Time")
    ts_w[, Time := format(Time, format = "%Y-%m-%d %H:%M:%S")]
  }
  
  # Save the data in wide format
  parquet_2_save_wide <- file.path(path_out, paste0(folder_name, "_wide.parquet"))
  arrow::write_parquet(as.data.frame(ts_w), parquet_2_save_wide)
  cat(cp(
    paste0(
      "The wide format has been saved as ",
      "'", path_relative(parquet_2_save_wide, path), "'"
    ),
    fg = 32
  ), "\n", sep = "")
}


# Make a spreadsheet output for primitive data checking purposes
ts_l[, .(
  Plate = first(Plate),
  Description = first(Description),
  Unit = first(Unit),
  Start = min(TimeStamp),
  End = max(TimeStamp),
  Min = min(Value) |> round(3),
  Time_min = TimeStamp[which.min(Value)],
  Max = max(Value) |> round(3),
  Time_max = TimeStamp[which.max(Value)]
), by = .(folder, Name, ts_id, CSV)][
  order(folder, Name, ts_id)
] |> fwrite(file.path(path_out, "data_range_dt.tsv"), sep = "\t")

cat(
  "Time elapsed:\t",
  round(difftime(Sys.time(), time_start, units = "secs"), digits = 3),
  " seconds.\n\n"
)
