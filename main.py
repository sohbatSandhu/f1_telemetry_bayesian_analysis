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
    lap_main_path = f"data/main/laps_all_{gp_year}_{gp_location}_{session_type[0]}.csv"
    df_laps.to_csv(lap_main_path, index=False)
    print("Data Saved to", lap_main_path)
    
    micro_telemetry_path = f"data/main/telemetry_micro_all_{gp_year}_{gp_location}_{session_type[0]}_m100.csv", 
    df_telemetry.to_csv(micro_telemetry_path, index=False)
    print("Data Saved to", micro_telemetry_path)