import os
from data_collection import build_datasets

if __name__ == "__main__":
    # Grand Prix Parameters
    gp_year = 2023
    gp_circuit_name = "Yas Marina Circuit"
    gp_location = "AbuDhabi"
    session_type = "Race"

    # ensure directory exists
    os.makedirs("data/main", exist_ok=True)
    
    # build datasets
    df_laps, df_telemetry = build_datasets(
        year=gp_year, circuit_name=gp_circuit_name
    )

    # store as csv files
    df_laps.to_csv(
        f"data/main/laps_all_{gp_year}_{gp_location}_{session_type[0]}.csv", 
        index=False
    )
    df_telemetry.to_csv(
        f"data/main/telemetry_micro_all_{gp_year}_{gp_location}_{session_type[0]}_m100.csv", 
        index=False
    )