
rm(list = ls(all.names = TRUE))

library(httr)  # Useful tools for working with HTTP organised by HTTP verbs
# library(data.table)  # Fast operations on large data frames

time_start <- Sys.time()  # Start the timer


end_point <- "https://aquarius.orc.govt.nz/AQUARIUS/Publish/v2"

# =========================================================================
# --- 'GetLocationDescriptionList': plate numbers (ID) <-> Names (Site) ---
# =========================================================================
desc_r <- GET(
  paste0(end_point, "/GetLocationDescriptionList"),
  authenticate("api-read", "PR98U3SKOczINoPHo7WM")
)
stop_for_status(desc_r, cat("Check the URL for the requested data!\n"))
desc <- content(desc_r)$LocationDescriptions
# plate_df <- rbindlist(lapply(desc, "[", c("Identifier", "Name")))[, unique(.SD)]
# setnames(plate_df, old = c("Identifier", "Name"), new = c("ID", "Site"))
select_col_1 <- c("Identifier", "Name")
new_name_1 <- c("ID", "Site")
plate_df <-
  lapply(desc, "[", name = select_col_1) |>
  do.call(rbind, args = _) |>
  unlist() |>
  matrix(ncol = length(select_col_1)) |>
  unique() |>
  as.data.frame() |>
  `colnames<-`(new_name_1)


# ============================================
# --- 'GetParameterList': Unit_id <-> Unit ---
# ============================================
param_r <- GET(
  paste0(end_point, "/GetParameterList"),
  authenticate("api-read", "PR98U3SKOczINoPHo7WM")
)
stop_for_status(param_r, cat("Check the URL for the requested data!\n"))
param <- content(param_r)$Parameters
# param_df <- rbindlist(lapply(param, "[", c("Identifier", "UnitIdentifier")))[, unique(.SD)]
# setnames(param_df, old = c("Identifier", "UnitIdentifier"), new = c("Param", "Unit"))
select_col_2 <- c("Identifier", "UnitIdentifier")
new_name_2 <- c("Param", "Unit")
param_df <-
  lapply(param, "[", select_col_2) |>
  do.call(rbind, args = _) |>
  unlist() |>
  matrix(ncol = length(select_col_2)) |>
  unique() |>
  as.data.frame() |>
  `colnames<-`(new_name_2)


# ===================================
# --- Export the obtained information
# ===================================
path <- getwd()
path_info <- file.path(path, "info")
if (!dir.exists(path_info))
  dir.create(path_info)
# fwrite(plate_df, file.path(path_info, "plate_info.csv"))
# fwrite(param_df, file.path(path_info, "param_info.csv"))
write.csv(plate_df, file.path(path_info, "plate_info.csv"), row.names = FALSE)
write.csv(param_df, file.path(path_info, "param_info.csv"), row.names = FALSE)


message(
  "Time elapsed:\t",
  round(difftime(Sys.time(), time_start, units = "secs"), digits = 3),
  " seconds.\n\n"
)
