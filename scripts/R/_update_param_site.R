
rm(list = ls(all.names = TRUE))

library(httr)  # Useful tools for working with HTTP organised by HTTP verbs
library(data.table)  # Fast operations on large data frames

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
plate_df <- rbindlist(lapply(desc, "[", c("Identifier", "Name")))[, unique(.SD)]
setnames(plate_df, old = "Identifier", new = "Plate")


# ============================================
# --- 'GetParameterList': Unit_id <-> Unit ---
# ============================================
param_r <- GET(
  paste0(end_point, "/GetParameterList"),
  authenticate("api-read", "PR98U3SKOczINoPHo7WM")
)
stop_for_status(param_r, cat("Check the URL for the requested data!\n"))
param <- content(param_r)$Parameters
param_df <- rbindlist(lapply(param, "[", c("Identifier", "UnitIdentifier")))[, unique(.SD)]
setnames(param_df, old = c("Identifier", "UnitIdentifier"), new = c("Param", "Unit"))


# ===================================
# --- Export the obtained information
# ===================================
path <- getwd()
path_info <- file.path(path, "info")
if (!dir.exists(path_info))
  dir.create(path_info)
fwrite(plate_df, file.path(path_info, "plate_info.csv"))
fwrite(param_df, file.path(path_info, "param_info.csv"))


message(
  "Time elapsed:\t",
  round(difftime(Sys.time(), time_start, units = "secs"), digits = 3),
  " seconds.\n\n"
)
