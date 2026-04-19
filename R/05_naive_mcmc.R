# ==============================================================================
# 05_naive_mcmc.R
# - Implements a simple naive model using MCMC to estimate driver ability
# - Uses a simple linear model with driver ability and tyre/temperature effects
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

# fit naive model using stan
mod <- cmdstan_model("stan/05_naive_model.stan")

# fit model using MCMC
fit <- mod$sample(
  seed = 123,
  data = prep$stan_naive_train,
  chains = 2,
  parallel_chains = 2,
  iter_warmup = 2000,
  iter_sampling = 2000,
  adapt_delta = 0.90, # increase adapt_delta for better convergence
  max_treedepth = 12, # increase max_treedepth for better exploration
  init = 0.1, # initialize parameters to small values for better convergence
  refresh = 500
)

# save MCMC fit object for later analysis
fit$save_object("outputs/fits/naive_mcmc.rds")

# fit model using MCMC with fewer iterations for quick diagnostics
fit_check <- mod$sample(
  data = prep$stan_naive_train,
  chains = 2,
  parallel_chains = 2,
  iter_warmup = 200,
  iter_sampling = 200,
  adapt_delta = 0.95,
  max_treedepth = 17,
  init = 0.1,
  refresh = 200
)

# save MCMC fit check object for quick diagnostics
fit_check$save_object("outputs/fits/naive_mcmc_check.rds")