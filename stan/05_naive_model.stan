data {
  int<lower=1> N;
  int<lower=1> D;
  int<lower=1> C;

  array[N] int<lower=1,upper=D> driver;
  array[N] int<lower=1,upper=C> compound;
  array[N] int<lower=0,upper=1> TrackStatus;

  vector[N] y; // log_lap_time

  vector[N] TyreLife_z;
  vector[N] TrackTemp_z;
}

parameters {
  real alpha0;

  real<lower=1e-6> sigma_driver;
  vector[D] z_driver;

  vector[C] beta_comp_raw;

  real beta_tyre;
  real beta_temp;

  real beta_status;

  real<lower=1e-6> sigma;
}

transformed parameters {
  vector[D] alpha_driver = sigma_driver * z_driver;
  vector[C] beta_comp = beta_comp_raw - mean(beta_comp_raw);
}

model {
  alpha0 ~ normal(0, 2);

  // Use exponential to avoid piling mass at ~0, and keep sigma > 0.
  sigma ~ exponential(1);
  sigma_driver ~ exponential(1);

  z_driver ~ std_normal();

  beta_comp_raw ~ normal(0, 0.5);

  beta_tyre  ~ normal(0, 0.5);
  beta_temp  ~ normal(0, 0.5);

  beta_status ~ normal(0, 0.5);

  for (n in 1:N) {
    real mu = alpha0
      + alpha_driver[driver[n]]
      + beta_comp[compound[n]]
      + beta_tyre * TyreLife_z[n]
      + beta_temp * TrackTemp_z[n]
      + beta_status * TrackStatus[n];

    y[n] ~ normal(mu, sigma);
  }
}

generated quantities {
  vector[N] log_lik;
  for (n in 1:N) {
    real mu = alpha0
      + alpha_driver[driver[n]]
      + beta_comp[compound[n]]
      + beta_tyre * TyreLife_z[n]
      + beta_temp * TrackTemp_z[n]
      + beta_status * TrackStatus[n];

    log_lik[n] = normal_lpdf(y[n] | mu, sigma);
  }
}
