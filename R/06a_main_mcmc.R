# ==============================================================================
# 06a_main_mcmc.R
# - Implements the main complex model using MCMC sampling
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

# fit model using MCMC
fit <- mod$sample(
  seed = 123,
  data = prep$stan_main_train,
  chains = 2,
  parallel_chains = 2,
  iter_warmup = 1000,
  iter_sampling = 1000,
  adapt_delta = 0.95, # increase adapt_delta for better convergence
  max_treedepth = 15, # increase max_treedepth for better exploration
  refresh = 500,
  init = 0.1 # initialize parameters to small values for better convergence
)

# save MCMC fit object for later analysis
fit$save_object("outputs/fits/main_mcmc.rds")

# fit model using MCMC
fit_check <- mod$sample(
  seed = 123,
  data = prep$stan_main_train,
  chains = 2,
  parallel_chains = 2,
  iter_warmup = 200,
  iter_sampling = 200,
  adapt_delta = 0.97, # increase adapt_delta for better convergence
  max_treedepth = 17, # increase max_treedepth for better exploration
  refresh = 20,
  init = 0.1 # initialize parameters to small values for better convergence
)

# save MCMC fit object for later analysis
fit_check$save_object("outputs/fits/main_mcmc_check.rds")
