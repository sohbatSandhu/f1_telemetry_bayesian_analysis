data {
  int<lower=1> N;
  int<lower=1> D;
  int<lower=1> T;
  int<lower=1> M;
  int<lower=1> C;

  array[N] int<lower=1,upper=D> driver;
  array[N] int<lower=1,upper=T> team;
  array[N] int<lower=1,upper=M> micro;
  array[N] int<lower=1,upper=C> compound;

  array[N] int<lower=0,upper=1> TrackStatus;

  vector[N] y;

  vector[N] TyreLife_z;
  vector[N] TrackTemp_z;
  vector[N] TyreLife_x_TrackTemp;

  int<lower=1> K;
  matrix[N, K] X;

  int<lower=1> ppc_n;
  array[ppc_n] int<lower=1,upper=N> ppc_idx;
}

parameters {
  real alpha0;

  real<lower=1e-6> sigma_driver;
  vector[D] z_driver;

  real<lower=1e-6> sigma_team;
  vector[T] z_team;

  real<lower=1e-6> sigma_micro;
  vector[M] z_micro;

  vector[C] beta_comp_raw;

  real beta_tyre;
  real beta_temp;
  real beta_inter;

  vector[K] beta_X;
  real beta_status;

  real<lower=1e-6> sigma;
  real<lower=2, upper=60> nu;
}

transformed parameters {
  vector[D] alpha_driver = sigma_driver * z_driver;
  vector[T] gamma_team   = sigma_team * z_team;
  vector[M] delta_micro  = sigma_micro * z_micro;

  vector[C] beta_comp = beta_comp_raw - mean(beta_comp_raw);
}

model {
  alpha0 ~ normal(0, 2);

  // scale priors (more stable than half-normal at exactly 0)
  sigma ~ exponential(1);
  sigma_driver ~ exponential(1);
  sigma_team ~ exponential(1);
  sigma_micro ~ exponential(1);

  z_driver ~ std_normal();
  z_team ~ std_normal();
  z_micro ~ std_normal();

  beta_comp_raw ~ normal(0, 0.5);

  beta_tyre  ~ normal(0, 0.5);
  beta_temp  ~ normal(0, 0.5);
  beta_inter ~ normal(0, 0.5);

  beta_X ~ normal(0, 0.5);
  beta_status ~ normal(0, 0.5);

  nu ~ uniform(2, 60);

  for (n in 1:N) {
    real mu = alpha0
      + alpha_driver[driver[n]]
      + gamma_team[team[n]]
      + delta_micro[micro[n]]
      + beta_comp[compound[n]]
      + beta_tyre * TyreLife_z[n]
      + beta_temp * TrackTemp_z[n]
      + beta_inter * TyreLife_x_TrackTemp[n]
      + dot_product(beta_X, X[n])
      + beta_status * TrackStatus[n];

    y[n] ~ student_t(nu, mu, sigma);
  }
}

generated quantities {
  vector[N] log_lik;
  vector[ppc_n] y_rep;

  real driver_frac = square(sigma_driver) / (square(sigma_driver) + square(sigma_team));
  real team_frac   = 1 - driver_frac;

  for (n in 1:N) {
    real mu = alpha0
      + alpha_driver[driver[n]]
      + gamma_team[team[n]]
      + delta_micro[micro[n]]
      + beta_comp[compound[n]]
      + beta_tyre * TyreLife_z[n]
      + beta_temp * TrackTemp_z[n]
      + beta_inter * TyreLife_x_TrackTemp[n]
      + dot_product(beta_X, X[n])
      + beta_status * TrackStatus[n];

    log_lik[n] = student_t_lpdf(y[n] | nu, mu, sigma);
  }

  for (j in 1:ppc_n) {
    int n = ppc_idx[j];
    real mu = alpha0
      + alpha_driver[driver[n]]
      + gamma_team[team[n]]
      + delta_micro[micro[n]]
      + beta_comp[compound[n]]
      + beta_tyre * TyreLife_z[n]
      + beta_temp * TrackTemp_z[n]
      + beta_inter * TyreLife_x_TrackTemp[n]
      + dot_product(beta_X, X[n])
      + beta_status * TrackStatus[n];

    y_rep[j] = student_t_rng(nu, mu, sigma);
  }
}
