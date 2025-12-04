#!/usr/bin/env Rscript

library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(arrow)
library(lubridate)

# Define directories relative to repo
repo_dir   <- here::here()
raw_dir    <- file.path(repo_dir, "data/raw")
latest_dir <- file.path(repo_dir, "data/latest")

if (!dir.exists(latest_dir)) dir.create(latest_dir, recursive = TRUE)

message("Processing raw Druid JSON files...")

# Get all raw JSON files
files <- list.files(raw_dir, pattern = "\\.json$", full.names = TRUE)

if (length(files) == 0) {
  stop("No raw Druid JSON files found.")
}

# Function to extract GPS points from one JSON file
process_file <- function(f) {
  message("Reading ", f)
  
  dat <- jsonlite::fromJSON(f, simplifyVector = TRUE) %>%
    dplyr::select(argos_id, timestamp, longitude, latitude, altitude)
  
  # Standardize fields
  gps <- dat %>% as_tibble() %>%
    rename(
      uuid = argos_id,
      lat = latitude,
      lon = longitude
    ) %>%
    mutate(
      timestamp = ymd_hms(timestamp, quiet = TRUE),
    ) %>%
    arrange(timestamp)
  
  return(gps)
}

# Process all files
all_tracks <- purrr::map_dfr(files, process_file)

message("Total GPS points: ", nrow(all_tracks))

# Write outputs
csv_path <- file.path(latest_dir, "druid_tracks.csv")
parquet_path <- file.path(latest_dir, "druid_tracks.parquet")

readr::write_csv(all_tracks, csv_path)
arrow::write_parquet(all_tracks, parquet_path)

message("Written: ", csv_path)
message("Written: ", parquet_path)