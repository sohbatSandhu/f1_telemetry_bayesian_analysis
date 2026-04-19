# ==============================================================================
# 04_model_prep.R
# - Prepares data for modeling in Stan
# - Creates train/test splits and standardizes continuous variables
# - Saves prepared data as RDS for use in modeling scripts
# ==============================================================================

# load libraries
library(dplyr)
library(readr)

# directory for model prep outputs
if (!dir.exists("outputs")) {
  dir.create("outputs", recursive = TRUE)
}

# Load processed data
telemetry_df <- read_csv("data/processed/telemetry_data.csv")
laps_df <- read_csv("data/processed/lap_data.csv")

# Safety checks
stopifnot(all(is.finite(telemetry_df$log_time)))
stopifnot(all(telemetry_df$TimeSeconds > 0))
stopifnot(all(is.finite(laps_df$log_lap_time)))
stopifnot(all(laps_df$LapTimeSeconds > 0))

# Helper function to split laps by driver into train/test sets
split_laps_by_driver <- function(laps, train_prop = 0.8) {
  # laps: data frame with driver_id, LapNumber (unique rows)
  laps |>
    group_by(driver_id) |>
    mutate(
      u = runif(dplyr::n()),
      split = if_else(u < train_prop, "train", "test")
    ) |>
    ungroup()
}

# ==============================================================================
# 1. Create lap-level dataset for naive model
# - Response: log_lap_time
# - Predictors: TyreLife, TrackTemp, compound_id, TrackStatus
# - Random effect: driver
# ==============================================================================

# Keep only what naive needs
laps_base <- laps_df |>
  transmute(
    y = log_lap_time,
    driver = as.integer(driver_id),
    compound = as.integer(compound_id),
    LapNumber = as.integer(LapNumber),
    TrackStatus = as.integer(TrackStatus),
    TyreLife = as.numeric(TyreLife),
  ) |>
  filter(is.finite(y)) |>
  mutate(TrackStatus = if_else(TrackStatus != 0, 1L, 0L))

# Add TrackTemp to lap-level from telemetry (mean within lap)
lap_temp <- telemetry_df |>
  select(driver_id, LapNumber, TrackTemp) |>
  group_by(driver_id, LapNumber) |>
  summarise(TrackTemp = mean(TrackTemp, na.rm = TRUE), .groups = "drop")

laps_base <- laps_base |>
  left_join(lap_temp, by = c("driver" = "driver_id", "LapNumber")) |>
  mutate(TrackTemp = as.numeric(TrackTemp)) |>
  filter(is.finite(TrackTemp))

# Split by (driver, LapNumber)
lap_groups <- laps_base |>
  distinct(driver, LapNumber) |>
  rename(driver_id = driver) |>
  split_laps_by_driver(train_prop = 0.8)

laps_base <- laps_base |>
  left_join(lap_groups |> transmute(driver = driver_id, LapNumber, split),
    by = c("driver", "LapNumber")
  )

laps_train <- laps_base |> filter(split == "train")
laps_test <- laps_base |> filter(split == "test")

# Standardize continuous predictors using TRAIN stats
scale_with_train <- function(x_train, x_all) {
  m <- mean(x_train)
  s <- sd(x_train)
  if (!is.finite(s) || s == 0) s <- 1
  (x_all - m) / s
}

laps_base <- laps_base |>
  mutate(
    TyreLife_z  = scale_with_train(laps_train$TyreLife, TyreLife),
    TrackTemp_z = scale_with_train(laps_train$TrackTemp, TrackTemp),
  )

laps_train <- laps_base |> filter(split == "train")
laps_test <- laps_base |> filter(split == "test")

# Stan list for naive lap-level
make_stan_naive_lap <- function(d) {
  list(
    N = nrow(d),
    D = max(d$driver),
    C = max(d$compound),
    driver = d$driver,
    compound = d$compound,
    TrackStatus = d$TrackStatus,
    y = d$y,
    TyreLife_z = d$TyreLife_z,
    TrackTemp_z = d$TrackTemp_z
  )
}

stan_naive_train <- make_stan_naive_lap(laps_train)
stan_naive_test <- make_stan_naive_lap(laps_test)

# ==============================================================================
# 2. Create micro-sector-level dataset for main model
# - Response: log_time
# - Predictors: Throttle, Brake, Speed, TyreLife,
#               TrackTemp, compound_id, TrackStatus
# - Random effects: driver, team, micro-sector, compound
# - Optional: downsample micro-sectors for faster modeling
# =============================================================================

# Main model base rows
micro_base <- telemetry_df |>
  transmute(
    y = log_time,
    driver = as.integer(driver_id),
    team = as.integer(team_id),
    micro = as.integer(micro_sector),
    compound = as.integer(compound_id),
    LapNumber = as.integer(LapNumber),
    TrackStatus = as.integer(TrackStatus),
    TyreLife = as.numeric(TyreLife),
    TrackTemp = as.numeric(TrackTemp),
    Throttle = as.numeric(Throttle) / 100,
    Brake = as.numeric(Brake) / 100,
    Speed = as.numeric(Speed),
    split = NA_character_
  ) |>
  filter(is.finite(y)) |>
  filter(
    is.finite(TyreLife), is.finite(TrackTemp),
    is.finite(Throttle), is.finite(Brake), is.finite(Speed)
  ) |>
  mutate(
    TrackStatus = if_else(TrackStatus != 0, 1L, 0L)
  )

# ---- Choose laps per driver for main model (downsampling knob) ----
# Set to NA to keep ALL laps; set to 6–10 to reduce runtime.
LAPS_PER_DRIVER_MAIN <- 8

# Compound coverage requirement in TRAIN
# allow fallback to 1 if 2 is too strict given lap-level downsampling
MIN_COMPOUNDS_PER_DRIVER_TRAIN <- 1

# First create lap-level table for sampling
lap_table <- micro_base |>
  distinct(driver, LapNumber, compound)

if (is.na(LAPS_PER_DRIVER_MAIN)) {
  chosen_laps <- lap_table |> distinct(driver, LapNumber)
} else {
  chosen_laps <- lap_table |>
    distinct(driver, LapNumber, compound) |>
    group_by(driver) |>
    # sample laps, keep compound diversity by sampling across compound groups
    group_modify(\(dd, ...) {
      # dd has driver fixed
      laps_unique <- dd |>
        distinct(LapNumber, compound) |>
        arrange(compound, LapNumber)

      # Try: take at least one lap from each compound first, then fill remaining
      by_comp <- split(laps_unique, laps_unique$compound)
      seed_laps <- lapply(by_comp, \(x) x$LapNumber[1]) |>
        unlist() |>
        unique()

      remaining_needed <- max(0, LAPS_PER_DRIVER_MAIN - length(seed_laps))
      remaining_pool <- setdiff(unique(laps_unique$LapNumber), seed_laps)

      fill_laps <- ifelse(
        remaining_needed > 0,
        sample(remaining_pool,
               size = min(remaining_needed, length(remaining_pool))),
        integer(0)
      )

      tibble(LapNumber = unique(c(seed_laps, fill_laps)))
    }) |>
    ungroup()
}

micro_small <- micro_base |>
  semi_join(chosen_laps, by = c("driver", "LapNumber"))

# Split by laps within each driver
micro_groups <- micro_small |>
  distinct(driver, LapNumber) |>
  rename(driver_id = driver) |>
  split_laps_by_driver(train_prop = 0.8)

micro_small <- micro_small |>
  left_join(micro_groups |> transmute(driver = driver_id, LapNumber, split),
    by = c("driver", "LapNumber")
  ) |>
  select(-split.x) |>
  rename(split = split.y)

micro_train <- micro_small |> filter(split == "train")
micro_test <- micro_small |> filter(split == "test")

# Enforce compound coverage in TRAIN per driver (best-effort)
compound_count <- micro_train |>
  distinct(driver, compound) |>
  count(driver, name = "n_compounds")

drivers_bad <- compound_count |>
  filter(n_compounds < MIN_COMPOUNDS_PER_DRIVER_TRAIN) |>
  pull(driver)

if (length(drivers_bad) > 0) {
  message(
    "WARNING: Some drivers have < ", MIN_COMPOUNDS_PER_DRIVER_TRAIN,
    " compounds in TRAIN after downsampling/split: ",
    paste(drivers_bad, collapse = ", "),
    ". Consider increasing LAPS_PER_DRIVER_MAIN or relaxing requirement to 1."
  )
}

# Standardize main predictors using TRAIN stats
micro_small <- micro_small |>
  mutate(
    TyreLife_z = scale_with_train(micro_train$TyreLife, TyreLife),
    TrackTemp_z = scale_with_train(micro_train$TrackTemp, TrackTemp),
    Throttle_z = scale_with_train(micro_train$Throttle, Throttle),
    Brake_z = scale_with_train(micro_train$Brake, Brake),
    Speed_z = scale_with_train(micro_train$Speed, Speed),
    TyreLife_x_TrackTemp = TyreLife_z * TrackTemp_z
  )

micro_train <- micro_small |> filter(split == "train")
micro_test <- micro_small |> filter(split == "test")

# PPC subset indices
make_ppc_idx <- function(N, ppc_n = 800) {
  ppc_n <- min(ppc_n, N)
  sample.int(N, size = ppc_n)
}

# Helper to create data list for main Stan model
make_stan_main <- function(d, ppc_n = 800) {
  X <- cbind(d$Throttle_z, d$Brake_z, d$Speed_z)
  N <- nrow(d)
  idx <- make_ppc_idx(N, ppc_n)

  list(
    N = N,
    D = max(d$driver),
    T = max(d$team),
    M = max(d$micro),
    C = max(d$compound),
    driver = d$driver,
    team = d$team,
    micro = d$micro,
    compound = d$compound,
    TrackStatus = d$TrackStatus,
    y = d$y,
    TyreLife_z = d$TyreLife_z,
    TrackTemp_z = d$TrackTemp_z,
    TyreLife_x_TrackTemp = d$TyreLife_x_TrackTemp,
    K = ncol(X),
    X = X,
    ppc_n = length(idx),
    ppc_idx = idx
  )
}

# Create Stan data lists for main model
stan_main_train <- make_stan_main(micro_train, ppc_n = 800)
stan_main_test <- make_stan_main(micro_test, ppc_n = 800)

# Save everything for downstream fit scripts
prep <- list(
  naive_lap_train = laps_train,
  naive_lap_test = laps_test,
  stan_naive_train = stan_naive_train,
  stan_naive_test = stan_naive_test,
  main_micro_train = micro_train,
  main_micro_test = micro_test,
  stan_main_train = stan_main_train,
  stan_main_test = stan_main_test,
  settings = list(
    LAPS_PER_DRIVER_MAIN = LAPS_PER_DRIVER_MAIN,
    MIN_COMPOUNDS_PER_DRIVER_TRAIN = MIN_COMPOUNDS_PER_DRIVER_TRAIN
  )
)

# Save prepared data as RDS for use in modeling scripts
saveRDS(prep, "outputs/model_prep.rds")

# Confirmation splits
cat("Naive lap-level N (train/test): ",
    nrow(laps_train), " / ", nrow(laps_test), "\n")
cat("Main micro-level N (train/test): ",
    nrow(micro_train), " / ", nrow(micro_test), "\n")
