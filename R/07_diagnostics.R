# ==============================================================================
# 07_diagnostics.R
# - Performs diagnostics and model comparison for the fitted MCMC and VI models
# - Includes trace plots, rank histograms, PPC, and LOO comparisons
# - Diagnostics focused on driver ability and driver-vs-team separation
# ==============================================================================

# load libraries
library(dplyr)
library(readr)
library(posterior)
library(bayesplot)
library(loo)
library(ggplot2)

# ensure output/figs directory for diagnostics exists
if (!dir.exists("outputs/diagnostics")) {
  dir.create("outputs/diagnostics", recursive = TRUE)
}
if (!dir.exists("figs/diagnostics")) {
  dir.create("figs/diagnostics", recursive = TRUE)
}

# Load prepared data and fitted models
prep <- readRDS("outputs/model_prep.rds")

fit_naive <- readRDS("outputs/fits/naive_mcmc.rds")
fit_main <- readRDS("outputs/fits/main_mcmc.rds")
fit_vi <- readRDS("outputs/fits/main_vi_meanfield.rds")

# helper to extract vector draws for indexed parameters
# (e.g. alpha_driver[1], alpha_driver[2], ...)
extract_indexed_draws <- function(draws, base, K) {
  posterior::as_draws_df(draws) |>
    dplyr::select(dplyr::all_of(sprintf("%s[%d]", base, 1:K))) |>
    as.matrix()
}

# helper to summarize effects with mean and 90% intervals
summarize_effects <- function(mat) {
  tibble(
    mean = colMeans(mat),
    lo90 = apply(mat, 2, quantile, 0.05),
    hi90 = apply(mat, 2, quantile, 0.95),
    id = seq_len(ncol(mat))
  )
}

# ==============================================================================
# 1. Naive model: driver ability (lap-level)
# ==============================================================================
draws_naive <- fit_naive$draws()

# Save summary table (Rhat/ESS for key params)
naive_key <- c("sigma_driver", "sigma", "beta_tyre", "beta_temp", "beta_status")
naive_sum <- posterior::summarise_draws(draws_naive) |>
  dplyr::filter(variable %in% naive_key)
write_csv(naive_sum, "outputs/diagnostics/naive_key_summary.csv")

# Driver effects plot
D_naive <- prep$stan_naive_train$D
alpha_driver_pars <- sprintf("alpha_driver[%d]", 1:D_naive)
alpha_naive <- posterior::summarise_draws(draws_naive) |>
  dplyr::filter(variable %in% alpha_driver_pars)

p_naive_drv <- ggplot(alpha_naive, aes(
  x = reorder(as.factor(variable), -mean), y = mean
)) +
  geom_point(color = "#1f78b4") +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0, lineend = "round") +
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Naive model: driver ability (random intercept)",
    x = "alpha_driver[driver_id]",
    y = "alpha_driver (log-lap-time scale)"
  )

# Save driver ability plot for naive model
ggsave("figs/diagnostics/naive_driver_ability.png",
       p_naive_drv, width = 8, height = 5, dpi = 300)

# save driver ability summary table for naive model
write_csv(alpha_naive, "outputs/diagnostics/naive_driver_ability_summary.csv")

# Quick diagnostics plots for naive (focus only on key scale params)
ggsave(
  "figs/diagnostics/naive_trace_scales.png",
  bayesplot::mcmc_trace(draws_naive, pars = c("sigma_driver", "sigma")) +
    labs(title = "Naive model: trace plots for scale parameters") +
    theme_minimal(),
  width = 10, height = 5, dpi = 300
)

ggsave(
  "figs/diagnostics/naive_rankhist_scales.png",
  bayesplot::mcmc_rank_hist(draws_naive, pars = c("sigma_driver", "sigma")) +
    labs(title = "Naive model: rank histograms for scale parameters") +
    theme_minimal(),
  width = 10, height = 5, dpi = 300
)

# ==============================================================================
# 2. Main model: driver vs team separation (MCMC)
# ==============================================================================
draws_main <- fit_main$draws()

main_key <- c("sigma_driver", "sigma_team", "sigma_micro", "sigma",
              "nu", "driver_frac", "team_frac")
main_sum <- posterior::summarise_draws(draws_main) |>
  dplyr::filter(variable %in% main_key)
write_csv(main_sum, "outputs/diagnostics/main_key_summary.csv")

# Driver and team effects (posterior intervals)
D_main <- prep$stan_main_train$D
T_main <- prep$stan_main_train$T

alpha_main_pars <- sprintf("alpha_driver[%d]", 1:D_main)
gamma_main_pars <- sprintf("gamma_team[%d]", 1:T_main)

drv_main <- posterior::summarise_draws(draws_main) |>
  dplyr::filter(variable %in% alpha_main_pars) |>
  mutate(model = "Main (MCMC)", effect = "Driver")

team_main <- posterior::summarise_draws(draws_main) |>
  dplyr::filter(variable %in% gamma_main_pars) |>
  mutate(model = "Main (MCMC)", effect = "Team")

# Plot: driver ability (main MCMC)
p_main_drv <- ggplot(drv_main, aes(
  x = reorder(as.factor(variable), -mean), y = mean
)) +
  geom_point(color = "#33a02c") +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0, lineend = "round") +
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Main model (MCMC): driver ability",
    x = "alpha_driver[driver_id]",
    y = "Estimate (log-micro-time scale)"
  )

ggsave("figs/diagnostics/main_driver_ability_mcmc.png",
       p_main_drv, width = 7, height = 8, dpi = 200)

# Plot: team effects (main MCMC)
p_main_team <- ggplot(team_main, aes(
  x = reorder(as.factor(variable), -mean), y = mean
)) +
  geom_point(color = "#ff7f00") +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0, lineend = "round") +
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Main model (MCMC): team/car contribution",
    x = "gamma_team[team_id]",
    y = "Estimate (log-micro-time scale)"
  )

ggsave("figs/diagnostics/main_team_effect_mcmc.png", p_main_team, width = 7, height = 5.5, dpi = 200)

# Trace + rank hist for separation parameters
ggsave(
  "figs/diagnostics/main_trace_separation.png",
  bayesplot::mcmc_trace(draws_main, pars = c("sigma_driver", "sigma_team")) +
    labs(
      title = "Main model (MCMC): trace plots for driver vs team separation"
    ) +
    theme_minimal(),
  width = 10, height = 5, dpi = 300
)

ggsave(
  "figs/diagnostics/main_rankhist_separation.png",
  bayesplot::mcmc_rank_hist(draws_main,
                            pars = c("sigma_driver", "sigma_team")) +
    labs(
      title = "Main model (MCMC): rank histograms for separation parameters"
    ) +
    theme_minimal(),
  width = 10, height = 5, dpi = 300
)

# ==============================================================================
# 3. Main model: MCMC vs VI driver ranking comparison
# ==============================================================================
draws_vi <- fit_vi$draws()

vi_alpha_pars <- sprintf("alpha_driver[%d]", 1:D_main)
vi_gamma_pars <- sprintf("gamma_team[%d]", 1:T_main)

# Summarize driver and team effects for VI model
drv_vi <- posterior::summarise_draws(draws_vi) |>
  dplyr::filter(variable %in% vi_alpha_pars) |>
  mutate(model = "Main (VI)", effect = "Driver")
team_vi <- posterior::summarise_draws(draws_vi) |>
  dplyr::filter(variable %in% vi_gamma_pars) |>
  mutate(model = "Main (VI)", effect = "Team")

# Combined driver plot: MCMC vs VI
drv_cmp <- bind_rows(drv_main, drv_vi)

p_drv_cmp <- ggplot(drv_cmp, aes(
  x = reorder(as.factor(variable), -mean), y = mean, color = model
)) +
  geom_point(position = position_dodge(width = 0.5)) +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0,
                position = position_dodge(width = 0.5)) +
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Driver ability comparison: Main model MCMC vs VI",
    x = "alpha_driver[driver_id]",
    y = "Estimates (log-micro-time scale)"
  )

ggsave(
  "figs/diagnostics/main_driver_ability_mcmc_vs_vi.png",
  p_drv_cmp, width = 7.5, height = 9, dpi = 200
)

# Team comparison plot: MCMC vs VI
team_cmp <- bind_rows(team_main, team_vi)

p_team_cmp <- ggplot(team_cmp, aes(x = reorder(as.factor(variable), -mean), y = mean, color = model)) +
  geom_point(position = position_dodge(width = 0.5)) +
  geom_errorbar(aes(ymin = q5, ymax = q95), width = 0,
                position = position_dodge(width = 0.5)) +
  coord_flip() +
  theme_minimal() +
  labs(
    title = "Team/car effect comparison: Main model MCMC vs VI",
    x = "gamma_team[team_id]",
    y = "Estimates (log-micro-time scale)"
  )

ggsave(
  "figs/diagnostics/main_team_effect_mcmc_vs_vi.png",
  p_team_cmp, width = 7.5, height = 6, dpi = 200
)

# Save a small stability table: rank correlation of driver means
rank_m <- rank(-drv_main$mean, ties.method = "average")
rank_v <- rank(-drv_vi$mean, ties.method = "average")
rho <- cor(rank_m, rank_v, method = "spearman")

write_csv(
  tibble(metric = "spearman_rank_corr_driver_mean", value = rho),
  "outputs/diagnostics/main_mcmc_vs_vi_rankcorr.csv"
)

# ==============================================================================
# 4. LOO compare (main MCMC vs main VI)
# ==============================================================================
# Only if both have log_lik and you want a quick table
loo_naive <- loo::loo(fit_naive$draws("log_lik"))
loo_main  <- loo::loo(fit_main$draws("log_lik"))
loo_vi <- loo::loo(fit_vi$draws("log_lik"))

capture.output(loo_naive, file = "outputs/diagnostics/loo_naive.txt")
capture.output(loo_main,  file = "outputs/diagnostics/loo_main.txt")
capture.output(loo_vi,    file = "outputs/diagnostics/loo_vi.txt")

cmp <- loo::loo_compare(loo_vi, loo_main)
write.csv(as.data.frame(cmp), "outputs/diagnostics/loo_compare_vi_vs_main.csv")
