#!/usr/bin/env Rscript

library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(arrow)
library(stringdist)
library(lubridate)

# Define directories relative to repo
repo_dir   <- here::here()
raw_dir    <- file.path(repo_dir, "data/druid/gnss/raw")
env_dir    <- file.path(repo_dir, "data/druid/env/raw/")
latest_dir <- file.path(repo_dir, "data/druid/latest")

if (!dir.exists(latest_dir)) dir.create(latest_dir, recursive = TRUE)

message("Processing raw Druid JSON files...")

# Get all raw JSON files
files_gnss <- list.files(raw_dir, pattern = "\\.json$", full.names = TRUE)
files_env  <- list.files(env_dir, pattern = "\\.json$", full.names = TRUE)


if (length(files) == 0) {
  stop("No raw Druid JSON files found.")
}

# Function to extract GPS points from one JSON file
process_file <- function(f) {
  message("Reading ", f)
  
  gnss <- jsonlite::fromJSON(f, simplifyVector = TRUE) %>%
    dplyr::select(argos_id, timestamp, longitude, latitude, altitude) %>%
    mutate(timestamp = ymd_hms(timestamp, quiet = TRUE))
  
  env  <- jsonlite::fromJSON(files_env[which.min(stringdist(f, files_env, method = "lv"))], simplifyVector = TRUE) %>%
    dplyr::select(argos_id, timestamp, inner_temperature, ambient_light, inner_pressure, battery_voltage, odba) %>%
    mutate(timestamp = ymd_hms(timestamp, quiet = TRUE))
  
  pairs <- lapply(1:nrow(gnss), function(x) {
    tibble(ind = which.min(abs(as.numeric(difftime(gnss$timestamp[x], env$timestamp, units = "mins"))))) %>%
      mutate(dt  = abs(as.numeric(difftime(gnss$timestamp[x], env$timestamp, units = "mins")))[ind])
    }) %>% do.call("rbind", .) %>% group_split(ind) %>% lapply(function(y) {
      ifelse(y$dt == min(y$dt), y$ind, NA)
    }) %>% do.call("c", .)
  
  
  gps <- gnss %>% bind_cols(env[pairs,] %>% dplyr::select(-argos_id) %>% rename(timestamp_env = timestamp))
  
  return(gps)
}

# Process all files
all_tracks <- purrr::map_dfr(files_gnss, process_file)

rownames(all_tracks) <- NULL

message("Total GPS points: ", nrow(all_tracks))

# Write outputs
csv_path <- file.path(latest_dir, "druid_tracks.csv")
parquet_path <- file.path(latest_dir, "druid_tracks.parquet")

readr::write_csv(all_tracks, csv_path)
arrow::write_parquet(all_tracks, parquet_path)

message("Written: ", csv_path)
message("Written: ", parquet_path)
