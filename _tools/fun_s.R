
# https://adv-r.hadley.nz/functions.html
# https://r-pkgs.org/index.html

if (getRversion() < "4.3.0")
  stop("R version should be no older than 4.3.0! Upgrade R from https://cran.r-project.org")

library(data.table)  # Fast operations on large data frames
library(httr)  # Useful tools for working with HTTP organised by HTTP verbs

Sys.setenv(TZ = "Etc/GMT-12")  # Sys.unsetenv("TZ"); Sys.timezone()


path_relative <- function(path, to) {
  return(sub(path, replacement = "", pattern = file.path(to, "/"), fixed = TRUE))
}


ok_comma <- function(FUN) {
  # Keep the last comma for `c()`, `list()`, `data.frame()` -> use it in caution!
  #
  # Notes:
  #   https://www.r-bloggers.com/2013/03/r-and-the-last-comma/
  function(...) {
    arg_list <- as.list(sys.call())[-1L]
    len <- length(arg_list)
    if (len > 1L) {
      last <- arg_list[[len]]
      if (missing(last))
        arg_list <- arg_list[-len]
    }
    do.call(FUN, arg_list)
  }
}


cp <- function(s = "", display = 0, fg = 39, bg = 48) {
  # Colour print in the (RStudio) console for a string or frame-like object.
  #
  # Args:
  #   s: An object, default is an empty character ("").
  #   display, default = 0:
  #     - 0: default
  #     - 1: highlight
  #     - 2: blur
  #     - 3: italic
  #     - 4: underline
  #     - 5: flash?
  #     - 7: inverse?
  #     - 8: hide
  #     - 9: strikethrough
  #     - 22: non-bold
  #     - 24: non-underline
  #     - 25: non-flash?
  #     - 27: non-inverse?
  #   fg (foreground), default = 39:
  #     - 30: black
  #     - 31: red
  #     - 32: green
  #     - 33: yellow
  #     - 34: blue
  #     - 35: magenta
  #     - 36: cyan
  #     - 37: white
  #     - 38: delete and pause
  #   bg (background), default = 48:
  #     - 40: black
  #     - 41: red
  #     - 42: green
  #     - 43: yellow
  #     - 44: blue
  #     - 45: magenta
  #     - 46: cyan
  #     - 47: white
  #
  # Return:
  #   A string for color print in the console.
  #
  # Notes:
  #   https://stackoverflow.com/questions/287871/how-do-i-print-colored-text-to-the-terminal
  x <- paste(utils::capture.output(s), "\n", sep = "")
  x[1] <- paste0(" ", x[1])
  return(paste0("\033[", display, ";", fg, ";", bg, "m", x, "\033[0m"))
}


is_datetime <- function(x) {
  # Checks if an object is a date/datetime class
  #
  # Args:
  #   x: An object.
  #
  # Returns:
  #   A single logical value.
  return(any(class(x) %in% c("Date", "POSIXct", "POSIXt")))
}


xts_2_dt <- function(xts_obj) {
  # Convert xts object to a data.table object
  res_dt <- as.data.table(xts_obj, keep.rownames = TRUE)
  colnames(res_dt)[1] <- if (!ts_step(res_dt)) "Date" else "Time"
  return(res_dt)
}


.as_df <- function(DF) {
  # DataFrame-related function (output) from the type (dt/df/tbl) of the input DF.
  if (data.table::is.data.table(DF) || xts::is.xts(DF)) {
    return(data.table::as.data.table)
  } else if (tibble::is_tibble(DF)) {
    return(tibble::as_tibble)
  } else {
    return(as.data.frame)
  }
}


.trim_ts <- function(TS) {
  # Remove the complete empty rows for a time series (TS).
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   The time series (TS) with all completely empty rows removed.
  if (xts::is.xts(TS))
    TS <- xts_2_dt(TS)
  x <- as.data.table(TS)
  col_dt <- x[, names(.SD), .SDcols = is_datetime]
  col_v <- x[, names(.SD), .SDcols = !is_datetime]
  n <- length(col_v)
  row_rm <- which(rowSums(is.na(x[, .SD, .SDcols = col_v])) == n)
  x_rmd <- if (length(row_rm)) x[-row_rm] else x
  r <- x_rmd[, .SD, .SDcols = c(col_dt, col_v)][order(i), env = list(i = col_dt)]
  return(.as_df(TS)(r))
}


ts_step <- function(TS, minimum_time_step_in_second = 60L) {
  # Identify the time step for a time series (can be multiple columns in a wide format)
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #   minimum_time_step_in_second: The threshold for checking time step, default is 60 secs.
  #
  # Returns:
  #   0: daily time step (days start from zero o'clock),
  #   -1: time series is not in a regular time step,
  #   any integer greater than 0: time series is in a regular time step (in secs),
  #   NULL: time series doesn't contain any values (i.e., empty).
  t1 <- .trim_ts(TS)[[1]]
  if (any(c(0, 1) %in% length(t1)))
    return(message("Not enough data to determine steps -> `NULL` returned!"))
  if (is(t1, "Date")) {
    return(0L)
  } else {
    t2 <- as.numeric(difftime(t1[-1], t1[-length(t1)], units = "s"))
    t3 <- t2[t2 >= minimum_time_step_in_second]
    step_minimum <- min(t3)
    if (all(t3 %% step_minimum == 0)) return(step_minimum) else return(-1L)
  }
}


na_ts_insert <- function(TS) {
  # Padding the input time series (TS) by NA.
  #
  # Args:
  #   TS: A time series (this can be time series of regular or irregular time step).
  #
  # Returns:
  #   Padded time series for regular time-step TS;
  #   empty-row-removed time series for irregular time-step TS.
  if (xts::is.xts(TS))
    TS <- xts_2_dt(TS)
  x <- .trim_ts(data.table(TS))
  if (dim(x)[1]) {
    con <- ts_step(x)
    rng <- range(x[[1]], na.rm = TRUE)
    time_df <-
      if (con == 0) {
        data.table(seq.Date(from = rng[1], to = rng[2], by = 1))
      } else if (con > 0) {
        data.table(seq.POSIXt(from = rng[1], to = rng[2], by = con))
      } else {
        data.table(x[[1]])
      }
    names(time_df) <- names(x)[1]
    res <- merge.data.table(time_df, x, by = names(x)[1], all.x = TRUE)
  } else {
    res <- x
  }
  return(.as_df(TS)(res))
}


hourly_2_daily <- function(hts, day_starts_at = 0L, agg = mean, prop = 1.) {
  # Aggregate the hourly time series to daily time series using customised function.
  #
  # Args:
  #   hts: An hourly time series (for a single site).
  #   day_starts_at: What time (hour) a day starts - 0 o'clock by default.
  #     e.g., 9L means the output of daily time series by 9 o'clock!
  #   agg: Customised aggregation function - mean by default.
  #   prop: The ratio of the available data (within a day range).
  #
  # Returns:
  #   A frame of daily time series with an extra column of site name.
  is_wholenumber <- function(x, tol = sqrt(.Machine$double.eps)) abs(x - round(x)) < tol
  if (!is_wholenumber(day_starts_at) || day_starts_at < 0L || day_starts_at > 23L)
    stop("`day_starts_at` must be an integer in [0L, 23L]!\n")
  if (prop < 0 || prop > 1)
    stop("`prop` must be in [0, 1]!\n")
  hts_c <- as.data.table(na_ts_insert(hts))
  site_name <- names(hts_c)[2]
  setnames(hts_c, old = names(hts_c), new = c("Time", "Value"))
  tmp <- hts_c[, {
    Time_ <- Time - 3600 * (1L + day_starts_at)
    .(Date = as.Date(Time_, tz = "Etc/GMT-12"), Value)
  }]
  tmp[, Prop := sum(!is.na(Value)) / 24, Date]
  r <- tmp[Prop >= prop, .(Agg = agg(Value, na.rm = TRUE)), Date]
  x <- if (any(c(0, 1) %in% dim(r)[1])) r else na_ts_insert(r)
  x[, Site := site_name]
  setnames(x, old = "Agg", new = paste("Agg", deparse(substitute(agg)), sep = "_"))
  return(.as_df(hts)(x))
}


ts_info <- function(TS) {
  # Data dictionary for the input time series (TS).
  #
  # Args:
  #   TS: Single/multiple time series (regular or irregular time step).
  #
  # Returns:
  #   A data.frame on data availability and completion in percentages for each site.
  ts_w <- if (xts::is.xts(TS)) xts_2_dt(TS) else as.data.table(TS)
  names(ts_w)[1] <- "Time"
  con <- ts_step(ts_w)
  if (is.null(con)) return(NULL)
  empty_df <- data.table(Site = names(TS)[-1L])
  ts_l <- melt.data.table(ts_w, id.vars = "Time", variable.name = "Site", value.name = "V")
  info_df <- na.omit(ts_l)[, .(Start = min(Time), End = max(Time), n = .N), Site]
  d_yr <- 365.2422
  info_df[, Length_yr := as.numeric(difftime(End, Start, units = "d")) / d_yr]
  info_df <- merge.data.table(empty_df, info_df, by = "Site", all.x = TRUE, sort = FALSE)
  if (con == -1L) return(.as_df(TS)(info_df[, -"n"]))
  step_day <- if (con == 0L) 1L else con / (3600 * 24)
  info_df[, `:=`(N = Length_yr * d_yr + step_day, Length_yr = Length_yr + step_day / d_yr)]
  info_df[, `:=`(Completion = n * step_day / N * 100, n = NULL, N = NULL)]
  setnames(info_df, old = "Completion", new = "Completion_%")
  return(.as_df(TS)(info_df[]))
}


ts_2_list <- function(TS) {
  # Transform the time series (wide format) to a list of time series (each site).
  #
  # Args:
  #   TS: A time series (this can be time series of both regular or irregular time step).
  #
  # Returns:
  #   A list split by columns in the input TS.
  if (xts::is.xts(TS)) TS <- xts_2_dt(TS)
  x <- as.data.frame(TS)
  L <- vector(mode = "list", length = dim(x)[2] - 1L)
  for (i in seq_len(length(L)))
    L[[i]] <- .as_df(TS)(na_ts_insert(x[, c(1, i + 1)]))
  return(`names<-`(L, names(TS)[-1]))
}


ts_melt <- function(TS, value_name = "Value") {
  # Get the long format of the time series.
  #
  # Args:
  #   TS: A time series (this can be time series of both regular or irregular time step)
  #   value_name: The name of the numeric column in the output data.frame
  #
  # Returns:
  #   A long format of the time series.
  con <- ts_step(TS)
  if (is.null(con)) return(NULL)
  x <- as.data.table(na_ts_insert(TS))
  dt_name <- names(x[, .SD, .SDcols = is_datetime])
  y <- NULL
  for (ts_site in ts_2_list(x)) {
    site <- names(ts_site)[2]
    ts_site_long <- data.table(
      Site = site,
      To_be_replaced = ts_site[[dt_name]],
      Value = ts_site[[site]]
    )
    setnames(ts_site_long, old = "Value", new = value_name)
    y <- rbind(y, ts_site_long)
  }
  setnames(y, old = "To_be_replaced", new = dt_name)
  return(.as_df(TS)(y))
}


get_AQ <- function(url, ...) {
  # Connect ORC's AQ using 'GET' verb
  return(GET(url, authenticate("api-read", "PR98U3SKOczINoPHo7WM"), ...))
}


get_uid <- function(measurement, site) {
  # Get UniqueId from paste0(measurement, "@", "site").
  #
  # Args:
  #   measurement: char
  #     The format of {Parameter}.{Label}, such as:
  #       * Flow.WMHourlyMean
  #       * Discharge.MasterDailyMean
  #   site: char
  #     The {LocationIdentifier} behind a site name, such as:
  #       * WM0062
  #       * FA780
  #
  # Returns:
  # str | NULL
  #   * str: UniqueId str used for requesting time series (Aquarius)
  #   * `None`: the UniqueId cannot be located
  if (trimws(site) == "")
    stop("Provide a correct string value for 'Site'!", call. = FALSE)
  ts_id_site <- paste0(measurement, "@", site)
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  r <- get_AQ(
    paste0(end_point, "/GetTimeSeriesDescriptionList"),
    query = list(LocationIdentifier = site)
  )
  ts_desc <- content(r)$TimeSeriesDescriptions
  if (is.null(ts_desc))
    return(NULL)
  chk <- data.table(
    Identifier = sapply(ts_desc, "[[", 1),
    UniqueId = sapply(ts_desc, "[[", 2)
  )
  uid <- chk[Identifier == ts_id_site, UniqueId]
  return(if (length(uid)) uid else NULL)
}


.get_site_name <- function(site) {
  # Get the site name from Aquarius (for a plate number).
  #
  # Args:
  #   site: char
  #     The plate number of a site.
  #
  # Returns:
  #   A character of the site name (for a plate number).
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  url_part <- parse_url(paste0(end_point, "/GetLocationDescriptionList"))
  url_part$query <- list(LocationIdentifier = site)
  url_c <- URLencode(build_url(url_part))
  r <- get_AQ(url_c)
  tmp <- content(r)$LocationDescriptions
  if (length(tmp)) {
    return(tmp[[1]]$Name)
  } else {
    cat("\nThe site name for plate [", site, "] is NOT found!!!!\n\n", sep = "")
    return(NA_character_)
  }
}
get_site_name <- Vectorize(.get_site_name, SIMPLIFY = TRUE, USE.NAMES = FALSE)


get_url_uid <- function(uid, date_start = NA, date_end = NA) {
  # Makes the URL for getting the time series for a plate through UniqueId.
  q_list <- list(TimeSeriesUniqueId = uid)
  if (!is.na(date_start))
    q_list$QueryFrom <- paste0(
      as.Date(as.character(date_start), format = "%Y%m%d"),
      "T00:00:00.0000000+12:00"
    )
  if (!is.na(date_end))
    q_list$QueryTo <- paste0(
      as.Date(as.character(date_end), format = "%Y%m%d") + 1L,
      "T00:00:00.0000000+12:00"
    )
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  url_r <- parse_url(paste0(end_point, "/GetTimeSeriesCorrectedData"))
  url_r$query <- q_list
  return(URLencode(build_url(url_r)))
}


get_ts <- function(...) {
  # Obtains the time series for a plate using the UniqueId.
  r <- get_AQ(get_url_uid(...))
  L <- content(r)
  point_list <- content(r)$Points
  if (!length(point_list)) {
    message("\nNo time series is available\n")
    empty_df <- data.table(
      Timestamp = character(),
      Value = numeric(),
      Unit = character(),
      Identifier = character()
    )
    return(empty_df)
  }
  ts_df <- data.table(
    Timestamp = substr(sapply(L$Points, "[[", "Timestamp"), 1, 19),
    Value = unlist(sapply(L$Points, "[[", "Value"), use.names = FALSE),
    Unit = L$Unit,
    Identifier = paste0(L$Parameter, ".", L$Label, "@", L$LocationIdentifier)
  )
  return(ts_df)
}


get_url_AQ <- function(measurement, site, date_start = NA, date_end = NA) {
  # Generate a URL for requesting time series (from Aquarius).
  #
  # Args:
  #   measurement: char
  #     The format of {Parameter}.{Label}, such as:
  #       * Flow.WMHourlyMean
  #       * Discharge.MasterDailyMean
  #   site: char
  #     The {LocationIdentifier} behind a site name, such as:
  #       * WM0062
  #       * FA780
  #   date_start: int, optional (default as NA)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning.
  #   date_end: int, optional (default as NA)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end (4 days from current date on).
  #   raw_data: bool, optional
  #     Raw data (hourly volume in m^3) from Aquarius (extra info). Default is `FALSE`
  #
  # Returns:
  #   A string of a URL
  uid <- get_uid(measurement, site)
  return(get_url_uid(uid, date_start, date_end))
}


get_ts_AQ <- function(measurement, site, date_start = NA, date_end = NA) {
  # Get the time series for a single site specified by those defined in `get_url_AQ`
  url_c <- get_url_AQ(measurement, site, date_start, date_end)
  if (is.null(url_c)) {
    cat(
      "\n[", measurement, "@", site, "] -> No data available for [", site, "]!\n", sep = ""
    )
    return(data.table(Site = character(), Timestamp = character(), Value = numeric()))
  }
  r <- get_AQ(url_c)
  point_list <- content(r)$Points
  if (length(point_list)) {
    ts_raw <- data.table(Site = site, rbindlist(point_list))
    ts_raw[, Value := unlist(Value, use.names = FALSE)]
  } else {
    ts_raw <- data.table(Site = character(), Timestamp = character(), Value = numeric())
  }
  return(ts_raw[])
}


.clean_24h_datetime <- function(shit_datetime) {
  # Clean the 24:00:00 in a datetime string to a normal datetime string.
  #
  # Args:
  #   shit_datetime: char
  #     The first 19 characters for the input follow a format of "%Y-%m-%dT%H:%M:%S",
  #     and it is supposed to have shit (24:MM:SS) itself, like "2020-12-31T24:00:00"
  #
  # Returns:
  #   A normal datetime string:
  #     such as '2021-01-01T00:00:00' converted from the shit one presented above.
  #
  # Example:
  #   > require(data.table)
  #   > bad_dt_str <- "2020-12-31T24:00:00"
  #   > .clean_24h_datetime(bad_dt_str)
  #   [1] "2021-01-01T00:00:00"
  if (length(shit_datetime) > 1L)
    stop("This function accepts a single string ONLY!", call. = FALSE)
  if (is.na(shit_datetime))
    return(as.character(NA))
  str_19 <- substr(shit_datetime, 1L, 19L)
  dt_list <- tstrsplit(str_19, split = "T")
  date_str <- dt_list[[1]]
  time_str <- dt_list[[2]]
  h_ms <- tstrsplit(time_str, split = ":")
  h <- as.integer(h_ms[[1]])
  if (h == 24L) {
    m <- as.character(h_ms[[2]])
    s <- as.character(h_ms[[3]])
    fmt <- "%Y-%m-%dT%H:%M:%S"
    time_str_new <- paste(h - 1L, m, s, sep = ":")
    date_str_new <- paste(date_str, time_str_new, sep = "T")
    return(format(as.POSIXct(date_str_new, tz = "UTC", format = fmt) + 3600L, fmt))
  }
  return(str_19)
}
clean_24h_datetime <- Vectorize(.clean_24h_datetime, SIMPLIFY = TRUE, USE.NAMES = FALSE)


hourly_WU_AQ <- function(site_list, date_start = NA, date_end = NA, raw_data = FALSE) {
  # A wrapper of getting hourly rate for multiple water meters (from Aquarius).
  #
  # Args:
  #   site_list: char. A list of water meters' names.
  #   date_start: int, optional (default as `NA`)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end: int, optional (default as `NA`)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end (4 days from current date on).
  #   raw_data: bool, optional (default as `FALSE`)
  #     Whether return the raw data (in l/s) or not (in m^3/s) from Aquarius.
  #
  # Returns:
  #   A data.table of hourly abstraction
  site_list <- unique(site_list)
  ts_raw_list <- lapply(
    site_list,
    FUN = get_ts_AQ,
    measurement = "Flow.WMHourlyMean",
    date_start = date_start,
    date_end = date_end
  )
  ts_raw <- rbindlist(ts_raw_list)
  if (raw_data) {
    cat("\n\n", "Note: The (raw) hourly rate of take is in L/s!!!!", "\n\n", sep = "")
    return(ts_raw[])
  }
  cat("\n\n", "Note: The hourly rate of take is in m^3/s!!!!", "\n\n", sep = "")
  ts_raw[, let(
    Time = as.POSIXct(
      clean_24h_datetime(Timestamp),
      format = "%Y-%m-%dT%H:%M:%S",
      tz = "Etc/GMT-12"
    ),
    Value = Value / 1e3
  )]
  ts_w <- dcast(ts_raw, Time ~ Site, value.var = "Value")
  ts_e <- data.table(Time = as.POSIXct(character(), tz = "Etc/GMT-12"))
  ts_e[, (site_list) := NA_real_]
  ts_dt <- na_ts_insert(rbind(ts_e, ts_w, fill = TRUE)[order(Time)])
  return(ts_dt[])
}


daily_WU_AQ <- function(site_list, date_start = NA, date_end = NA, raw_data = FALSE) {
  # A wrapper of getting daily rate for multiple water meters (from Aquarius).
  #
  # Args:
  #   site_list: char. A list of water meters' names.
  #   date_start: int, optional (default as `NA`)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end: int, optional (default as `NA`)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end.
  #   raw_data: bool, optional (default as `FALSE`)
  #     Whether return the raw data (daily volume in m^3) or not (in m^3/s) from Aquarius.
  #
  # Returns:
  #   A data.table of daily abstraction
  site_list <- unique(site_list)
  ts_raw_list <- lapply(
    site_list,
    FUN = get_ts_AQ,
    measurement = "Abstraction Volume.WMDaily",
    date_start = date_start,
    date_end = date_end
  )
  ts_raw <- rbindlist(ts_raw_list)
  if (raw_data) {
    cat("\n\n", "Note: The (raw) daily take volume is in m^3!!!!", "\n\n", sep = "")
    return(ts_raw[])
  }
  cat("\n\n", "Note: The daily rate of take is in m^3/s!!!!", "\n\n", sep = "")
  ts_raw[, let(
    Date = as.Date(substr(Timestamp, 1, 10), format = "%Y-%m-%d"),
    Value = Value / 86400
  )]
  ts_w <- dcast(ts_raw, Date ~ Site, value.var = "Value")
  ts_e <- data.table(Date = as.Date(character()))
  ts_e[, (site_list) := NA_real_]
  ts_dt <- na_ts_insert(rbind(ts_e, ts_w, fill = TRUE)[order(Date)])
  return(ts_dt[])
}


daily_Flo_AQ <- function(site_list, date_start = NA, date_end = NA, raw_data = FALSE) {
  # A wrapper of getting daily flow rate for multiple flow recorders (from Aquarius).
  #
  # Args:
  #   site_list: char. A list of plate numbers (of flow recorders).
  #   date_start: int, optional (default as `NA`)
  #     Start date of the requested data. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data from its very beginning. The default is NA.
  #   date_end: int, optional (default as `NA`)
  #     End date of the request data date. It follows '%Y%m%d' When specified.
  #     Otherwise, request the data till its end.
  #   raw_data: bool, optional (default as `FALSE`)
  #     Whether return the raw data (Timestamp in string) from Aquarius.
  #
  # Returns:
  #   A data.table of daily flow rate.
  site_list <- unique(site_list)
  ts_raw_list <- lapply(
    site_list,
    FUN = get_ts_AQ,
    measurement = "Discharge.MasterDailyMean",
    date_start = date_start,
    date_end = date_end
  )
  ts_raw <- rbindlist(ts_raw_list)
  if (raw_data) {
    cat("\n\n", "Note: The (raw) daily flow is in m^3!!!!", "\n\n", sep = "")
    return(ts_raw[])
  }
  cat("\n\n", "Note: The daily flow rate is in m^3/s!!!!", "\n\n", sep = "")
  ts_raw[, Date := as.Date(substr(Timestamp, 1, 10), format = "%Y-%m-%d")]
  ts_w <- dcast(ts_raw, Date ~ Site, value.var = "Value")
  ts_e <- data.table(Date = as.Date(character()))
  ts_e[, (site_list) := NA_real_]
  ts_dt <- na_ts_insert(rbind(ts_e, ts_w, fill = TRUE)[order(Date)])
  sn <- get_site_name(site_list)
  sn[which(is.na(sn))] <- site_list[which(is.na(sn))]
  setnames(ts_dt, old = site_list, new = sn)
  return(ts_dt[])
}


.field_data_AQ_s <- function(plate, parameters = NULL) {
  # Get the field visit data (from Aquarius) for a single plate (site).
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  url_ <- paste0(end_point, "/GetFieldVisitReadingsByLocation?LocationIdentifier=", plate)
  if (!is.null(parameters))
    for (p in parameters)
      url_ <- paste(url_, p, sep = "&Parameters=")
  url_ <- URLencode(url_)
  r <- get_AQ(url = url_)
  reading_list <- content(r)$FieldVisitReadings
  if (!length(reading_list))
    return(NULL)
  data_all <- rbindlist(reading_list, fill = TRUE, use.names = TRUE, idcol = "LID")
  reading <- cbind(data.table(Plate = plate), data_all[, unique(.SD), .SDcols = !is.list])
  fmt <- "%Y-%m-%dT%H:%M:%S"
  reading[, Time := as.POSIXct(clean_24h_datetime(Time), tz = "Etc/GMT-12", format = fmt)]
  col_list_type <- names(data_all[, .SD, .SDcols = is.list])
  df_list <- list()
  for (i in col_list_type) {
    tmp <- rbindlist(lapply(reading_list, "[[", i), fill = TRUE, idcol = "LID")
    names_to_check <- names(tmp[, .SD, .SDcols = is.list])
    if (length(names_to_check))
      for (ii in names_to_check)
        tmp[[ii]] <- unlist(tmp[[ii]], use.names = FALSE)
    names_tmp <- paste(i, names(tmp)[names(tmp) != "LID"], sep = ".")
    setnames(tmp, old = names(tmp)[names(tmp) != "LID"], new = names_tmp)
    df_list[[i]] <- tmp
  }
  final_list <- append(list(reading), df_list)
  f_cbind <- function(a, b) merge.data.table(a, b, by = "LID", all = TRUE)
  final_df <- Reduce(f_cbind, final_list)[order(Parameter, Time)]
  final_df[, Time := strftime(Time, format = fmt)]
  return(final_df[, -"LID"])
}


get_field_data_AQ <- function(plates, parameters = NULL) {
  # Get the field visit data (from Aquarius) for multi sites (plates).
  #
  # Args:
  #   plates: char. The list of plate(s).
  #   parameters: char.
  #     - Air Temp
  #     - Cond
  #     - Dis Oxygen Sat
  #     * Discharge
  #     - Dissolved Oxygen
  #     - GZF
  #     - Gas Pressure
  #     - Groundwater Level
  #     - Hydraulic Radius
  #     - Maximum Gauged Depth
  #     - NO3 (Dis)
  #     - O2 (Dis)
  #     - PM 10
  #     - Rainfall
  #     - Rainfall Depth
  #     - Sp Cond
  #     * Stage
  #     - Stage Change
  #     - Stage Offset
  #     - Tot Susp Sed
  #     - Turbidity (Form Neph)
  #     - Voltage
  #     - Water Surface Slope
  #     - Water Temp
  #     - Water Velocity
  #     - Wetted Perimeter
  #     - pH
  #     - pH Voltage
  #
  # Returns:
  #   A data.table of file measurements.
  plates <- unique(plates)
  field_data <- NULL
  for (plate in plates) {
    tmp <- .field_data_AQ_s(plate, parameters = parameters)
    if (is.null(tmp)) {
      cat("No field visit data for - [", plate, "] in the given parameter(s)\n", sep = "")
      next
    }
    field_data <- rbindlist(list(field_data, tmp), fill = TRUE)
  }
  if (!is.null(field_data))
    setnames(field_data, old = c("Value.Unit", "Value.Numeric"), new = c("Unit", "Value"))
  return(field_data[])
}


get_stage_flow_AQ <- function(plates) {
  # Get the field visit Stage-Discharge data (from Aquarius) for multi sites (plates).
  return(get_field_data_AQ(plates, parameters = c("Stage", "Discharge")))
}


.get_field_hydro_s <- function(plate) {
  # A helper function for `get_field_hydro_AQ()`
  end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"
  url_ <- paste0(end_point, "/GetFieldVisitDataByLocation?LocationIdentifier=", plate)
  url_ <- URLencode(url_)
  r <- get_AQ(url_)
  data_list <- content(r)$FieldVisitData
  empty_df <- data.table(
    Identifier = character(),
    LocationIdentifier = character(),
    MeasurementTime = character(),
    GradeCode = character(),
    Measurement = character(),
    Unit = character(),
    Numeric = numeric()
  )
  if (is.null(unlist(sapply(data_list, "[", "DischargeActivities"))))
    return(empty_df)
  cois <- c("Identifier", "LocationIdentifier", "DischargeActivities")
  tmp <- data.table()
  for (coi in cois)
    tmp[, j := sapply(data_list, "[", coi), env = list(j = coi)]
  tmp[, let(
    Identifier = as.character(Identifier),
    LocationIdentifier = as.character(LocationIdentifier)
  )]
  tmp[, Len_list := lapply(DischargeActivities, length)]
  a <- tmp[Len_list >= 1L]
  if (!a[, .N])
    return(empty_df)
  a <- a[rep(seq(1, nrow(a)), Len_list)]
  a[, Index := 1:.N, by = Identifier]
  tmp_list <- list()
  for (i in a[, .I]) {
    idx <- a[i, Index]
    list_i <- a[[i, "DischargeActivities"]]
    tmp_list[[i]] <- list_i[[idx]]
  }
  a[, let(DischargeActivities = tmp_list, Len_list = NULL, Index = NULL)]
  a <- a[lapply(DischargeActivities, length) > 0]
  dis_act_lst <- list()
  for (i in a[, .I])
    dis_act_lst[[i]] <- unlist(a[i, DischargeActivities]) |> as.list()
  fmt <- "%Y-%m-%dT%H:%M:%S"
  null_as_char <- function(x) if (is.null(x)) NA_character_ else x
  base_df <- data.table(
    Identifier = a[, Identifier],
    LocationIdentifier = a[, LocationIdentifier],
    MeasurementTime = fcoalesce(
      sapply(dis_act_lst, "[[", "DischargeSummary.MeasurementTime") |>
        substr(1, 19) |>
        as.POSIXct(tz = "Etc/GMT-12", format = fmt),
      sapply(dis_act_lst, "[[", "MeasurementTime") |>
        substr(1, 19) |>
        as.POSIXct(tz = "Etc/GMT-12", format = fmt)
    ),
    GradeCode = fcoalesce(
      sapply(dis_act_lst, "[[", "DischargeSummary.GradeCode") |>
        sapply(null_as_char),
      sapply(dis_act_lst, "[[", "GradeCode") |>
        sapply(null_as_char)
    )
  )
  if (any(is.na(base_df[, MeasurementTime]))) {
    fill_values_mt <- base_df[
      !is.na(MeasurementTime),
      .(fill_mt = unique(MeasurementTime)),
      by = .(Identifier, LocationIdentifier)
    ]
    base_df[fill_values_mt, MeasurementTime := fifelse(
      is.na(MeasurementTime),
      i.fill_mt,
      MeasurementTime
    ), on = .(Identifier, LocationIdentifier)]
  }
  if (any(is.na(base_df[, GradeCode]))) {
    fill_values_gc <- base_df[
      !is.na(GradeCode),
      .(fill_gc = unique(GradeCode)),
      by = .(Identifier, LocationIdentifier, MeasurementTime)
    ]
    base_df[fill_values_gc, GradeCode := fifelse(
      is.na(GradeCode),
      i.fill_gc,
      GradeCode
    ), on = .(Identifier, LocationIdentifier, MeasurementTime)]
  }
  extract_value_char <- function(name_part)
    return(sapply(dis_act_lst, "[[", name_part) |> sapply(null_as_char) |> unname())
  flo_unit <- fcoalesce(
    extract_value_char("DischargeSummary.Discharge.Unit"),
    extract_value_char("Discharge.Unit")
  )
  flo_v <- fcoalesce(
    extract_value_char("DischargeSummary.Discharge.Numeric"),
    extract_value_char("Discharge.Numeric")
  )
  discharge_df <- copy(base_df)
  discharge_df[, let(Measurement = "Discharge", Unit = flo_unit, Numeric = flo_v)]
  mgh_unit <- fcoalesce(
    extract_value_char("DischargeSummary.MeanGageHeight.Unit"),
    extract_value_char("MeanGageHeight.Unit")
  )
  mgh_v <- fcoalesce(
    extract_value_char("DischargeSummary.MeanGageHeight.Numeric"),
    extract_value_char("MeanGageHeight.Numeric")
  )
  mgh_df <- copy(base_df)
  mgh_df[, let(Measurement = "MeanGageHeight", Unit = mgh_unit, Numeric = mgh_v)]
  width_unit <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.Width.Unit"),
    extract_value_char("PointVelocityDischargeActivities.Width.Unit"),
    extract_value_char("Width.Unit")
  )
  width_v <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.Width.Numeric"),
    extract_value_char("PointVelocityDischargeActivities.Width.Numeric"),
    extract_value_char("Width.Numeric")
  )
  width_df <- copy(base_df)
  width_df[, let(Measurement = "Width", Unit = width_unit, Numeric = width_v)]
  area_unit <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.Area.Unit"),
    extract_value_char("PointVelocityDischargeActivities.Area.Unit"),
    extract_value_char("Area.Unit")
  )
  area_v <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.Area.Numeric"),
    extract_value_char("PointVelocityDischargeActivities.Area.Numeric"),
    extract_value_char("Area.Numeric")
  )
  area_df <- copy(base_df)
  area_df[, let(Measurement = "Area", Unit = area_unit, Numeric = area_v)]
  vel_unit <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.VelocityAverage.Unit"),
    extract_value_char("PointVelocityDischargeActivities.VelocityAverage.Unit"),
    extract_value_char("VelocityAverage.Unit")
  )
  vel_v <- fcoalesce(
    extract_value_char("AdcpDischargeActivities.VelocityAverage.Numeric"),
    extract_value_char("PointVelocityDischargeActivities.VelocityAverage.Numeric"),
    extract_value_char("VelocityAverage.Numeric")
  )
  velocity_df <- copy(base_df)
  velocity_df[, let(Measurement = "VelocityAverage", Unit = vel_unit, Numeric = vel_v)]
  hydro_df <- rbind(discharge_df, mgh_df, width_df, area_df, velocity_df)
  hydro_df[, Numeric := as.numeric(Numeric)]
  hydro_df[Numeric == -1, Numeric := NA_real_]
  hydro_df <- hydro_df[!is.na(Numeric)][order(Measurement, MeasurementTime)]
  hydro_df[, MeasurementTime := format(MeasurementTime, format = fmt)]
  return(hydro_df[])
}


get_field_hydro_AQ <- function(site_list) {
  # Get the field spot gauging data for multiple plates.
  #
  # Args:
  #   site_list: a character (vector)
  #     Plate numbers in a character vector
  #
  # Returns:
  #   A data.table of the field data for the following:
  #     * Discharge: "DischargeSummary"
  #     * MeanGageHeight: "DischargeSummary"
  #     * Width: "AdcpDischargeActivities" -> "PointVelocityDischargeActivities"
  #     * Area: "AdcpDischargeActivities" -> "PointVelocityDischargeActivities"
  #     * VelocityAverage: "AdcpDischargeActivities" -> "PointVelocityDischargeActivities"
  # Notes:
  #   This experimental function focuses on flow/stage/width/area/velocity ONLY.
  #     * Aquarius API: "/GetFieldVisitDataByLocation".
  #     * Data is retrieved with priorities presented as above.
  #     * The returned data.table is empty if no field data is available.
  site_list <- unique(site_list)
  hydro_df <- data.table(
    Identifier = character(),
    LocationIdentifier = character(),
    MeasurementTime = character(),
    GradeCode = character(),
    Measurement = character(),
    Unit = character(),
    Numeric = numeric()
  )
  for (plate in site_list) {
    cat("Getting the field visit hydro data for - ", "[", plate, "]...\n", sep = "")
    tmp_df <- .get_field_hydro_s(plate)
    if (!tmp_df[, .N]) {
      cat("\t-> No field hydro data for - ", "[", plate, "]!\n", sep = "")
      next
    }
    hydro_df <- rbind(hydro_df, tmp_df)
  }
  return(hydro_df)
}
