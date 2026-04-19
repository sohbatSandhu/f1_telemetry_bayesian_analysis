# ==============================================================================
# 02_read_data_wrangle.R
# - Read in telemetry and lap level data
# - Identify clean laps (no pitstops, early stops)
# - Identify missing microsectors for (driver, LapNumber) combination
# - Replace missing variables
#   - fill fixed variables (categorical)
#   - interpolate telemetry variables (categorical)
#   - if still missing (fall back using same micro sector mean)
# - Generate summary statistics and visualizations
# ==============================================================================

# =========================
# LIBRARIES
# =========================
library(dplyr)
library(tidyr)
library(readr)
library(zoo)

# ==============================================================================
# 1. Load telemetry data and lap level data
# ==============================================================================
telemetry_df <- read_csv(
  "data/main/telemetry_micro_all_2023_AbuDhabi_R_m100.csv", col_names = TRUE
)
laps_df <- read_csv("data/main/laps_all_2023_AbuDhabi_R.csv", col_names = TRUE)

# ==============================================================================
# 2. Initialize driver, team and compound factor keys
# ==============================================================================
driver_key <- tibble(
  Driver = c("VER", "PER", "HAM", "RUS", "LEC", "SAI", "NOR", "PIA",
             "ALO", "STR", "GAS", "OCO",
             "ALB", "SAR", "TSU", "RIC", "BOT", "ZHO", "MAG", "HUL"),
  driver_id = 1:20
)

team_key <- tibble(
  Team = c("Red Bull Racing", "Mercedes", "Ferrari", "McLaren", # high tier
           "Aston Martin", "Alpine", # mid tier
           "Williams", "AlphaTauri", "Alfa Romeo", "Haas F1 Team"),
  team_id = 1:10
)

compound_key <- tibble(
  Compound = c("SOFT", "MEDIUM", "HARD"),
  compound_id = 1:3
)

# ==============================================================================
# 3. Identify clean laps
# - Should include all individuals racing (no pitstop, early stops)
# - Remove starting lap (slow lap as there cars move from no motion)
# ==============================================================================
pit_laps <- laps_df |>
  filter(!is.na(PitInTime) | !is.na(PitOutTime)) |>
  pull(LapNumber) |>
  unique()

# laps data
processed_laps_df <- laps_df |>
  filter(
    LapNumber >= 2, # exclude start lap
    LapNumber <= 57, # cars stopped earlier
    !LapNumber %in% pit_laps # pitting laps
  ) |>
  left_join(driver_key, by = "Driver") |>
  left_join(team_key, by = "Team") |>
  left_join(compound_key, by = "Compound") |>
  mutate(
    log_lap_time = log(LapTimeSeconds)
  ) |>
  arrange(driver_id, team_id, LapNumber)

# telemetry data
clean_telemetry_df <- telemetry_df |>
  filter(
    LapNumber >= 2, # exclude start lap
    LapNumber <= 57, # cars stopped earlier
    !LapNumber %in% pit_laps, # pitting laps
  ) |>
  left_join(driver_key, by = "Driver") |>
  left_join(team_key, by = "Team") |>
  left_join(unique(processed_laps_df[c("Driver", "LapNumber", "compound_id")]),
            by = c("Driver", "LapNumber")) |>
  mutate(
    micro_sector = micro_sector + 1
  ) |>
  arrange(driver_id, team_id, LapNumber, micro_sector)

# ==============================================================================
# 4. Identify missing microsectors for (driver, LapNumber) combination
# ==============================================================================
all_sectors <- 1:100
missing_rows <- clean_telemetry_df |>
  group_by(driver_id, LapNumber) |>
  summarize(
    missing_micro_sector = list(setdiff(all_sectors, micro_sector)),
    n_missing = length(setdiff(all_sectors, micro_sector)),
    .groups = "drop"
  ) |>
  filter(n_missing > 0)

# ==============================================================================
# 5. Replace missing variables
# - fill fixed variables (categorical)
# - interpolate telemetry variables (categorical)
# - if still missing (fall back using same micro sector mean)
# ==============================================================================

# complete missing micro sectors
init_telemetry_df <- clean_telemetry_df |>
  group_by(driver_id, LapNumber) |>
  complete(micro_sector = 1:100) |>
  ungroup()

# fill fixed variables
imputed_telemetry_df <- init_telemetry_df |>
  group_by(driver_id, LapNumber) |>
  fill(
    Team,
    team_id,
    Driver,
    driver_id,
    compound_id,
    TyreLife,
    AirTemp,
    TrackTemp,
    LapTimeSeconds,
    .direction = "downup"
  ) |>
  ungroup()

# interpolate telemetry variables
interp_vars <- c("Speed", "Throttle", "Brake", "rpm", "TimeSeconds")
imputed_telemetry_df <- imputed_telemetry_df |>
  group_by(driver_id, LapNumber) |>
  arrange(micro_sector) |>
  mutate(across(
    all_of(interp_vars),
    ~ na.approx(.x, x = micro_sector, na.rm = FALSE)
  )) |>
  ungroup()

# same sector mean imputation for remaining missing values
driver_sector_means <- imputed_telemetry_df |>
  group_by(driver_id, micro_sector) |>
  summarize(
    across(
      all_of(interp_vars),
      \(x) mean(x, na.rm = TRUE)
    ),
    .groups = "drop"
  )

imputed_telemetry_df <- imputed_telemetry_df |>
  left_join(
    driver_sector_means,
    by = c("driver_id", "micro_sector"),
    suffix = c("", "_mean")
  )

for (v in interp_vars) {
  imputed_telemetry_df[[v]] <- ifelse(
    is.na(imputed_telemetry_df[[v]]),
    imputed_telemetry_df[[paste0(v, "_mean")]],
    imputed_telemetry_df[[v]]
  )
}

# complete telemetry data with imputed values
imputed_telemetry_df <- imputed_telemetry_df |>
  select(-ends_with("_mean")) |>
  mutate(
    log_time = log(TimeSeconds),
    log_lap_time = log(LapTimeSeconds)
  )

# ======================================================================
# 6. Save processed data to data/processed
# ======================================================================

# create directory if doesn't exist
if (!dir.exists("data/processed")) {
  dir.create("data/processed", recursive = TRUE)
}

# processed telemetry data
processed_telemetry_df <- imputed_telemetry_df |>
  select(
    Driver, driver_id, Team, team_id, compound_id, LapNumber, micro_sector,
    Speed, Throttle, Brake, rpm, TimeSeconds,
    AirTemp, TrackTemp, TyreLife
  ) |>
  arrange(driver_id, team_id, LapNumber, micro_sector)

write_csv(processed_telemetry_df, "data/processed/telemetry_data.csv")

# processed lap level data
processed_laps_df <- processed_laps_df |>
  select(
    Driver, driver_id, Team, team_id, LapNumber, Compound,
    compound_id, TyreLife, Stint, TrackStatus, LapTimeSeconds, log_lap_time
  ) |>
  arrange(driver_id, team_id, LapNumber)

write_csv(processed_laps_df, "data/processed/lap_data.csv")
