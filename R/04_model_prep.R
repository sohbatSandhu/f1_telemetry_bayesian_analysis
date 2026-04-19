# ==============================================================================
# 04_model_prep.R
# - Prepares data for modeling in Stan
# - Creates train/test splits and standardizes continuous variables
# - Saves prepared data as RDS for use in modeling scripts
# ==============================================================================

# load libraries
suppressStartupMessages(library(dplyr))
suppressStartupMessages(library(readr))

# directory for model prep outputs
if (!dir.exists("outputs")) {
  dir.create("outputs", recursive = TRUE)
}

# Load processed data
telemetry_df <- read_csv("data/processed/telemetry_data.csv")

# Safety checks
stopifnot(all(telemetry_df$TimeSeconds > 0))
stopifnot(all(is.finite(telemetry_df$log_time)))

# Build lap-group split ID (prevents leakage within a lap)
lap_groups <- telemetry_df |>
  distinct(driver_id, LapNumber) |>
  mutate(u = runif(dplyr::n()),
         split = if_else(u < 0.8, "train", "test"))

# Create model prep dataframes with necessary transformations
df <- telemetry_df |>
  left_join(lap_groups, by = c("driver_id", "LapNumber")) |>
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

    # driver inputs
    Throttle = as.numeric(Throttle) / 100,
    Brake = as.numeric(Brake) / 100,
    Speed = as.numeric(Speed),

    split = split
  ) |>
  filter(is.finite(y)) |>
  filter(is.finite(TyreLife), is.finite(TrackTemp),
         is.finite(Throttle), is.finite(Brake), is.finite(Speed)) |>
  mutate(
    TrackStatus = if_else(TrackStatus != 0, 1L, 0L)
  )

# Re-split after filtering
df_train <- df |> filter(split == "train")

# Helper function to standardize continuous variables using train stats
scale_with_train <- function(x_train, x_all) {
  m <- mean(x_train)
  s <- sd(x_train)
  if (!is.finite(s) || s == 0) s <- 1
  (x_all - m) / s # z-score
}

# z-score continuous vars using train stats
df <- df |>
  mutate(
    TyreLife_z  = scale_with_train(df_train$TyreLife,  TyreLife),
    TrackTemp_z = scale_with_train(df_train$TrackTemp, TrackTemp),
    Throttle_z  = scale_with_train(df_train$Throttle,  Throttle),
    Brake_z     = scale_with_train(df_train$Brake,     Brake),
    Speed_z     = scale_with_train(df_train$Speed,     Speed),
    TyreLife_x_TrackTemp = TyreLife_z * TrackTemp_z
  )

# Re-split after creating z columns
df_train <- df |> filter(split == "train")
df_test  <- df |> filter(split == "test")

# Helper to create Predictive Posterior Check for subset indices
make_ppc_idx <- function(N, ppc_n = 800) {
  ppc_n <- min(ppc_n, N)
  sample.int(N, size = ppc_n)
}

# Helper to create data list for Stan
make_stan_naive <- function(d) {
  list(
    N = nrow(d),
    D = max(d$driver),
    C = max(d$compound),

    driver = d$driver,
    compound = d$compound,
    TrackStatus = d$TrackStatus,

    y = d$y,

    TyreLife_z = d$TyreLife_z,
    TrackTemp_z = d$TrackTemp_z,
    TyreLife_x_TrackTemp = d$TyreLife_x_TrackTemp
  )
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
    team   = d$team,
    micro  = d$micro,
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

# Create list of prepared data for modeling
prep <- list(
  df_train = df_train,
  df_test  = df_test,
  stan_naive_train = make_stan_naive(df_train),
  stan_naive_test  = make_stan_naive(df_test),
  stan_main_train  = make_stan_main(df_train, ppc_n = 800),
  stan_main_test   = make_stan_main(df_test,  ppc_n = 800)
)

# Save prepared data as RDS for use in modeling scripts
saveRDS(prep, "outputs/model_prep.rds")
