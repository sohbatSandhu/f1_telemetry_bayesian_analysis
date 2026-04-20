# ==============================================================================
# 06b_main_vi.R
# - Implements the main complex model using Variational Inference
# - Uses a hierarchical model with driver, team, sector, and compound effects
# ==============================================================================

# load libraries
library(dplyr)
library(readr)
library(distr)
library(cmdstanr)

# ensure output directory for model fits exists
if (!dir.exists("outputs/fits")) {
  dir.create("outputs/fits", recursive = TRUE)
}

# load prepared data for modeling
prep <- readRDS("outputs/model_prep.rds")

# fit main model using stan
mod <- cmdstan_model("stan/06_main_model.stan")

# fit model using Variational Inference
fit_vi <- mod$variational(
  seed = 123,
  data = prep$stan_main_train,
  algorithm = "meanfield",
  iter = 20000,
  grad_samples = 4, # for gradient estimation
  elbo_samples = 100, # number of MC samples for ELBO estimation
  output_samples = 4000, # number of post-samples to draw from VI approximation
  refresh = 200,
  init = 0.1 # initialize parameters to small values for better convergence
)

# save VI fit object for later analysis
fit_vi$save_object("outputs/fits/main_vi_meanfield.rds")