#!/usr/bin/env Rscript

library(jsonlite)
library(dplyr)
library(purrr)
library(readr)
library(arrow)
library(stringdist)
library(lubridate)
library(sf)

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


if (length(files_gnss) == 0) {
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


### further processing

druid_ids <- tibble(
  argos_id = c(
    "69098ff3448200353a4c028f",
    "69098ff2448200353a4c028d",
    "69098ff1448200353a4c028b",
    "69098ff1448200353a4c028a",
    "69098ff0448200353a4c0289",
    "69098ff0448200353a4c0288",
    "69098ff3448200353a4c028e",
    "69098fef448200353a4c0287",
    "69098ff2448200353a4c028c"
  ),
  uuid = c(
    "06e6",
    "06e8",
    "06e9",
    "06ee",
    "06e3",
    "06eb",
    "06ed",
    "06e0",
    "06e5"
  ),
  species = c(
    "Gentoo penguin",
    "Gentoo penguin",
    "Adélie penguin",
    "Adélie penguin",
    "Adélie penguin",
    "Adélie penguin",
    "Adélie penguin",
    "Adélie penguin",
    "Gentoo penguin"
  )
)

tracks_sf <- all_tracks %>%
  mutate(timestamp = as.POSIXct(timestamp)) %>%
  filter(timestamp >= as.POSIXct("2025-11-27 23:00:58", tz = "UTC")) %>%
  left_join(druid_ids) %>% st_as_sf(coords = c("longitude", "latitude"), crs = 4326)

saveRDS(tracks_sf, file = paste0(latest_dir, "/tracks_sf.rds"))

message("Written: ", paste0(latest_dir, "/tracks_sf.rds"))
