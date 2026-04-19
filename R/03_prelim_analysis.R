# ======================================================================
# 03_prelim_analysis.R
# - Preliminary analysis of clean laps and telemetry data
# - Creates summary tables and visualizations
# - Focus on relationships between tyre life, track temperature and performance
# ======================================================================

# load libraries
library(dplyr)
library(ggplot2)
library(readr)
library(gridExtra)
library(scales)

# ensure figs directory for visualizations exists
if (!dir.exists("figs")) {
  dir.create("figs", recursive = TRUE)
}

# ensure output directory for summary stats exists
if (!dir.exists("outputs")) {
  dir.create("outputs", recursive = TRUE)
}

# ======================================================================
# 1. load processed data
# ======================================================================
telemetry_df <- read_csv("data/processed/telemetry_data.csv")
laps_df <- read_csv("data/processed/lap_data.csv")

# ======================================================================
# 2. Get lap level summary per driver
# ======================================================================
lap_summary_table <- laps_df |>
  group_by(driver_id) |>
  reframe(
    n_laps = n(),
    mean_lap_time = round(mean(LapTimeSeconds, na.rm = TRUE), 4),
    sd_lap_time = round(sd(LapTimeSeconds, na.rm = TRUE), 4),
    min_lap_time = round(min(LapTimeSeconds, na.rm = TRUE), 4),
    q25_75 = paste0(
      round(quantile(LapTimeSeconds, c(0.25, 0.75), na.rm = TRUE), 4),
      collapse = ","
    ),
    total_time = round(sum(LapTimeSeconds, na.rm = TRUE), 2)
  ) |>
  arrange(driver_id)

write_csv(lap_summary_table, "outputs/lap_summary_by_driver.csv")

# ======================================================================
# 3. Separate teams by performance tiers based on season car performance
# - Keys for tier assignment to be used to separate into par plots
# - comparison:
#   - Row 1: (1 top teams, 1 mid team, 1 back team)
#   - Row 1: (1 top teams, 1 mid team, 1 back team)
#   - Row 1: (2 top teams, 2 back team)
# ======================================================================
tiers_key <- tibble(
  team_id = c(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
  team_name = c(
    "Red Bull Racing", "Mercedes", "Ferrari", "McLaren", # high tier
    "Aston Martin", "Alpine", # mid tier
    "Williams", "AlphaTauri", "Alfa Romeo", "Haas F1 Team"
  ),
  performance_tier = c(
    "Top", "Top", "Mid", "Mid",
    "Mid", "Mid", "Back", "Back",
    "Back", "Back"
  )
)

team_groups <- list(
  c(1, 4, 6, 10),    # Row 1: 2 top, 1 mid, 1 back
  c(2, 5, 9),      # Row 2: 1 top, 1 mid, 1 back
  c(3, 7, 8)        # Row 3: 1 top, 2 back
)

# ======================================================================
# 4. Create Driver vs Compound Lap Times and plots by performance tier
# ======================================================================
base_theme <- theme_minimal(base_size = 10) +
  theme(
    plot.title = element_text(face = "bold", size = 12),
    panel.grid.minor = element_blank()
  )

# side-by-side boxplots for compound comparison by performance tier
compound_colors <- c(
  "SOFT" = "#ee4949", "MEDIUM" = "#d5ae3b", "HARD" = "#A9A9A9"
)

plot_list <- list()

for (i in seq_along(team_groups)) {

  teams <- team_groups[[i]]

  filtered_df <- laps_df |>
    filter(team_id %in% teams) |>
    arrange(team_id, driver_id)

  # ensure x-axis is ordered by team_id and driver_id
  p <- ggplot(filtered_df, aes(
    x = reorder(Driver, team_id * driver_id),
    y = LapTimeSeconds,
    fill = Compound
  )) +
    geom_boxplot(outlier.alpha = 0.2) +
    scale_fill_manual(values = compound_colors) +
    labs(
      title = paste(
        "Compound vs Lap Time -",
        paste(tiers_key$team_name[match(teams, tiers_key$team_id)],
              collapse = ", ")
      ),
      x = "Driver",
      y = "Lap Time (seconds)"
    ) +
    theme_minimal() +
    theme(
      legend.position = "top",
      axis.text.x = element_text(angle = 45, hjust = 1)
    )

  plot_list[[i]] <- p
}

# Display stacked plots
combined_plot <- grid.arrange(
  grobs = plot_list,
  ncol = 1
)

# save row 1 plot
ggsave(
  filename = "figs/compound_comparison_by_team_group_row1.png",
  plot = plot_list[[1]], width = 6, height = 5, dpi = 300
)

# save combined plot
ggsave(
  filename = "figs/compound_comparison_by_team_group.png",
  plot = combined_plot, width = 8, height = 15, dpi = 300
)

# ======================================================================
# 5. Heatmap for per micro-sector time across laps for drivers
# ======================================================================
pivot_df <- telemetry_df |>
  group_by(driver_id, micro_sector) |>
  summarize(TimeSeconds = mean(TimeSeconds, na.rm = TRUE), .groups = "drop") |>
  left_join(
    laps_df |> select(driver_id, Driver, team_id) |> distinct(),
    by = "driver_id"
  )
vmax <- max(pivot_df$TimeSeconds, na.rm = TRUE)
vmin <- min(pivot_df$TimeSeconds, na.rm = TRUE)

heatmap_plot <- ggplot(pivot_df, aes(
  y = Driver,
  x = micro_sector,
  fill = TimeSeconds
)) +
  geom_tile() +
  scale_fill_gradient2(
    low = "#54ac21",
    mid = "white",
    high = "#b29618",
    midpoint = (vmax + vmin) / 2,
    name = "Time (s)"
  ) +
  labs(
    title = "Micro-sector Performance Heatmap",
    x = "Micro-sector",
    y = "Driver"
  ) +
  coord_cartesian(xlim = c(0, 101)) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 8)
  )

# save heatmap
ggsave(
  filename = "figs/micro_sector_performance_heatmap.png",
  plot = heatmap_plot, width = 6, height = 5, dpi = 300
)


# =======================================================================
# 7. Raw time plots for key variables (tyre life, track temp)
# - scatter + loess smooth
# =======================================================================

# 1. TyreLife per compound vs Time
tyre_plot <- ggplot(telemetry_df, aes(
  x = TyreLife, y = TimeSeconds, color = compound_id
)) +
  geom_point(alpha = 0.2) +
  geom_smooth(method = "loess", se = FALSE) +
  labs(
    title = "Tyre Life vs Micro-sector Time",
    x = "Tyre Life",
    y = "Time (s)",
    color = "Compound"
  ) +
  scale_color_manual(
    values = c("1" = "#ee4949", "2" = "#d5ae3b", "3" = "#A9A9A9"),
    labels = c("1" = "SOFT", "2" = "MEDIUM", "3" = "HARD"),
    name = "Compound"
  ) +
  base_theme

ggsave("figs/tyre_vs_time.png", tyre_plot, width = 8, height = 5)

# 2. Temperature vs Time
temp_plot <- ggplot(telemetry_df, aes(
  x = TrackTemp, y = TimeSeconds, color = compound_id
)) +
  geom_point(alpha = 0.2) +
  geom_smooth(method = "loess", se = FALSE) +
  labs(
    title = "Track Temperature vs Performance",
    x = "Track Temp",
    y = "Time (s)",
    color = "Compound"
  ) +
  base_theme

ggsave("figs/temp_vs_time.png", temp_plot, width = 8, height = 5)