#!/usr/bin/env Rscript

library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(arrow)
library(lubridate)

# Define directories relative to repo
repo_dir <- here::here()
raw_dir  <- file.path(repo_dir, "data/raw_druid")
latest_dir <- file.path(repo_dir, "data/latest_druid")

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
  
  dat <- jsonlite::fromJSON(f, simplifyVector = TRUE)
  
  # Most Druid files have structure:
  # $data$gps$data or $items$track$gps etc.
  # We'll search for lat/lon automatically
  
  gps <- dat %>% 
    jsonlite::flatten() %>%
    purrr::pluck("data", "gps", "data", .default = NULL)
  
  if (is.null(gps)) {
    gps <- dat %>% purrr::pluck("gps", "data", .default = NULL)
  }
  if (is.null(gps)) {
    gps <- dat %>% purrr::pluck("track", "gps", .default = NULL)
  }
  
  if (is.null(gps)) {
    warning("No GPS data found in ", f)
    return(NULL)
  }
  
  gps <- as.data.frame(gps)
  
  # Add device UUID from filename
  uuid <- stringr::str_extract(basename(f), "gps_raw_([a-z0-9]+)") %>%
    stringr::str_remove("gps_raw_")
  
  gps$uuid <- uuid
  
  # Standardize fields
  gps <- gps %>%
    rename(
      timestamp = time,
      lat = latitude,
      lon = longitude
    ) %>%
    mutate(
      timestamp = ymd_hms(timestamp, quiet = TRUE),
      uuid = as.character(uuid)
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